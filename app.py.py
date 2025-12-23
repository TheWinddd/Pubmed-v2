# streamlit_app.py
# PubMed Exporter - Export to CSV/XLSX

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import quote_plus

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PUBMED_WEB_BASE = "https://pubmed.ncbi.nlm.nih.gov/"

# --- PubMed query equivalents (for NCBI E-utilities) ---
TERM_TEXT_FILTERS_SB = {
    "Abstract": "hasabstract",
    "Free full text": "free full text[sb]",
    "Full text": "full text[sb]",
}

TERM_ATTR_FILTERS_SB = {"Associated data": "data[sb]"}

TERM_ARTICLE_TYPES = {
    "Clinical Trial": '"clinical trial"[pt]',
    "Meta-Analysis": '"meta-analysis"[pt]',
    "Randomized Controlled Trial": '"randomized controlled trial"[pt]',
    "Review": "review[pt]",
    "Systematic Review": "systematic[sb]",
    "Books and Documents": '"pubmed books"[sb]',
}


@dataclass
class NcbiConfig:
    email: str
    api_key: Optional[str] = None
    tool: str = "pubmed_streamlit_exporter"
    requests_per_second: float = 2.8
    timeout_sec: int = 45


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6, backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _throttle(last_ts: float, rps: float) -> float:
    min_interval = 1.0 / max(rps, 0.1)
    now = time.time()
    wait = (last_ts + min_interval) - now
    if wait > 0:
        time.sleep(wait)
    return time.time()


def _combine_clauses(clauses: List[str], mode: str) -> Optional[str]:
    clauses = [c.strip() for c in clauses if c and c.strip()]
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    op = " OR " if mode == "OR" else " AND "
    return "(" + op.join(clauses) + ")"


def build_eutils_query(
    base_term: str,
    text_filters: List[str],
    attr_filters: List[str],
    article_types: List[str],
    within_group_mode: str = "OR",
) -> str:
    parts: List[str] = [base_term.strip()]
    text_clause = _combine_clauses([TERM_TEXT_FILTERS_SB[t] for t in text_filters if t in TERM_TEXT_FILTERS_SB], within_group_mode)
    if text_clause:
        parts.append(text_clause)
    attr_clause = _combine_clauses([TERM_ATTR_FILTERS_SB[a] for a in attr_filters if a in TERM_ATTR_FILTERS_SB], within_group_mode)
    if attr_clause:
        parts.append(attr_clause)
    type_clause = _combine_clauses([TERM_ARTICLE_TYPES[pt] for pt in article_types if pt in TERM_ARTICLE_TYPES], within_group_mode)
    if type_clause:
        parts.append(type_clause)
    return " AND ".join([p for p in parts if p])


def compute_date_range(date_mode: str, start: Optional[date], end: Optional[date]) -> Tuple[Optional[str], Optional[str]]:
    if date_mode == "None":
        return None, None
    today = date.today()
    def fmt(d: date) -> str:
        return d.strftime("%Y/%m/%d")
    if date_mode == "1 year":
        start_d = date(today.year - 1, today.month, min(today.day, 28))
        return fmt(start_d), fmt(today)
    if date_mode == "5 years":
        start_d = date(today.year - 5, today.month, min(today.day, 28))
        return fmt(start_d), fmt(today)
    if date_mode == "10 years":
        start_d = date(today.year - 10, today.month, min(today.day, 28))
        return fmt(start_d), fmt(today)
    if date_mode == "Custom range" and start and end:
        if start > end:
            start, end = end, start
        return fmt(start), fmt(end)
    return None, None


def esearch_history(
    session: requests.Session,
    term: str,
    cfg: NcbiConfig,
    mindate: Optional[str],
    maxdate: Optional[str],
    datetype: str = "pdat",
) -> Tuple[int, str, str]:
    params = {
        "db": "pubmed", "term": term, "retmode": "json",
        "usehistory": "y", "retmax": 0,
        "email": cfg.email, "tool": cfg.tool,
    }
    if cfg.api_key:
        params["api_key"] = cfg.api_key
    if mindate:
        params["mindate"] = mindate
    if maxdate:
        params["maxdate"] = maxdate
    if datetype:
        params["datetype"] = datetype
    r = session.get(EUTILS_BASE + "esearch.fcgi", params=params, timeout=cfg.timeout_sec)
    r.raise_for_status()
    data = r.json()["esearchresult"]
    count = int(data.get("count", 0))
    webenv = data.get("webenv")
    query_key = data.get("querykey")
    if not webenv or not query_key:
        raise RuntimeError("ESearch did not return WebEnv/query_key.")
    return count, webenv, query_key


def efetch_xml_batch(
    session: requests.Session,
    webenv: str,
    query_key: str,
    cfg: NcbiConfig,
    retstart: int,
    retmax: int,
) -> str:
    params = {
        "db": "pubmed", "webenv": webenv, "query_key": query_key,
        "retmode": "xml", "rettype": "abstract",
        "retstart": retstart, "retmax": retmax,
        "email": cfg.email, "tool": cfg.tool,
    }
    if cfg.api_key:
        params["api_key"] = cfg.api_key
    r = session.get(EUTILS_BASE + "efetch.fcgi", params=params, timeout=cfg.timeout_sec)
    r.raise_for_status()
    return r.text


def _safe_text(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def parse_pubmed_xml(xml_str: str) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_str)
    rows: List[Dict[str, str]] = []
    for art in root.findall(".//PubmedArticle"):
        row: Dict[str, str] = {}
        pmid = art.findtext(".//PMID") or ""
        row["pmid"] = pmid.strip()
        row["title"] = _safe_text(art.find(".//ArticleTitle"))
        row["journal"] = art.findtext(".//Journal/Title") or ""
        row["journal_iso"] = art.findtext(".//Journal/ISOAbbreviation") or ""
        
        pub_year, pub_date_raw = "", ""
        ad = art.find(".//ArticleDate")
        if ad is not None:
            y = ad.findtext("Year") or ""
            m = ad.findtext("Month") or ""
            d = ad.findtext("Day") or ""
            pub_year = y
            pub_date_raw = "-".join([x for x in [y, m, d] if x])
        if not pub_year:
            pd_elem = art.find(".//PubDate")
            if pd_elem is not None:
                y = pd_elem.findtext("Year") or ""
                md = pd_elem.findtext("MedlineDate") or ""
                pub_year = y or (md[:4] if md else "")
                pub_date_raw = y or md
        row["pub_year"] = pub_year
        row["pub_date"] = pub_date_raw
        
        doi = None
        for a in art.findall(".//ArticleId"):
            if a.attrib.get("IdType") == "doi" and (a.text or "").strip():
                doi = (a.text or "").strip()
                break
        row["doi"] = doi or ""
        
        author_names: List[str] = []
        for a in art.findall(".//AuthorList/Author"):
            last = a.findtext("LastName") or ""
            fore = a.findtext("ForeName") or a.findtext("Initials") or ""
            name = (last + " " + fore).strip()
            if name:
                author_names.append(name)
        row["authors"] = ", ".join(author_names)
        
        affs = []
        for aff in art.findall(".//AffiliationInfo/Affiliation"):
            t = _safe_text(aff)
            if t:
                affs.append(t)
        seen = set()
        affs2 = []
        for a in affs:
            if a not in seen:
                seen.add(a)
                affs2.append(a)
        row["affiliations"] = "\n".join(affs2)
        
        abs_elems = art.findall(".//Abstract/AbstractText")
        abstracts = []
        for abs_elem in abs_elems:
            label = abs_elem.attrib.get("Label", "")
            txt = _safe_text(abs_elem)
            if not txt:
                continue
            if label:
                abstracts.append(f"{label}: {txt}")
            else:
                abstracts.append(txt)
        row["abstract"] = "\n\n".join(abstracts)
        
        pts = [_safe_text(x) for x in art.findall(".//PublicationTypeList/PublicationType")]
        pts = [x for x in pts if x]
        row["publication_types"] = "; ".join(dict.fromkeys(pts))
        
        langs = [(_safe_text(x) or "").strip() for x in art.findall(".//Language")]
        langs = [x for x in langs if x]
        row["languages"] = "; ".join(dict.fromkeys(langs))
        
        kws = [_safe_text(x) for x in art.findall(".//KeywordList/Keyword")]
        kws = [x for x in kws if x]
        row["keywords"] = "; ".join(dict.fromkeys(kws))
        
        row["pubmed_url"] = f"{PUBMED_WEB_BASE}{row['pmid']}/" if row["pmid"] else ""
        rows.append(row)
    return rows


def fetch_pubmed(
    term: str,
    cfg: NcbiConfig,
    mindate: Optional[str],
    maxdate: Optional[str],
    max_records: int,
    batch_size: int,
    datetype: str = "pdat",
) -> pd.DataFrame:
    session = make_session()
    last_ts = 0.0
    last_ts = _throttle(last_ts, cfg.requests_per_second)
    count, webenv, query_key = esearch_history(session, term, cfg, mindate=mindate, maxdate=maxdate, datetype=datetype)
    target = min(count, max_records) if max_records > 0 else count
    if target == 0:
        return pd.DataFrame()
    rows: List[Dict[str, str]] = []
    fetched = 0
    while fetched < target:
        this_batch = min(batch_size, target - fetched)
        last_ts = _throttle(last_ts, cfg.requests_per_second)
        xml = efetch_xml_batch(session, webenv, query_key, cfg, retstart=fetched, retmax=this_batch)
        batch_rows = parse_pubmed_xml(xml)
        rows.extend(batch_rows)
        fetched += this_batch
    df = pd.DataFrame(rows)
    return df


# ============== STREAMLIT UI ==============

st.set_page_config(page_title="PubMed Exporter", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for better UI
st.markdown("""
<style>
    /* Hide default sidebar */
    [data-testid="stSidebar"] { display: none; }
    
    /* Main container */
    .main .block-container {
        padding-top: 2rem;
        max-width: 1200px;
    }
    
    /* Header styling */
    h1 {
        color: #1e88e5;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    /* Card style containers */
    .stExpander {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
    }
    
    /* Better buttons */
    .stButton > button {
        border-radius: 8px;
        font-weight: 500;
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div,
    .stNumberInput > div > div > input {
        border-radius: 6px;
    }
    
    /* Tags/chips for keywords */
    .keyword-tag {
        display: inline-flex;
        align-items: center;
        background: #e3f2fd;
        color: #1565c0;
        padding: 4px 12px;
        border-radius: 16px;
        margin: 4px;
        font-size: 14px;
    }
    
    /* Section headers */
    .section-header {
        color: #424242;
        font-size: 1.1rem;
        font-weight: 600;
        margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e0e0e0;
    }
    
    /* Stats card */
    .stats-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem 1.5rem;
        border-radius: 10px;
        text-align: center;
    }
    
    /* Download buttons area */
    .download-area {
        background: #f5f5f5;
        padding: 1.5rem;
        border-radius: 10px;
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🔬 PubMed Exporter")
st.caption("Tìm kiếm và xuất dữ liệu từ PubMed sang CSV/XLSX")

# Initialize session state
if "keywords_list" not in st.session_state:
    st.session_state["keywords_list"] = []
if "counts" not in st.session_state:
    st.session_state["counts"] = {}
if "df_all" not in st.session_state:
    st.session_state["df_all"] = pd.DataFrame()

# ============== SECTION 1: Keywords Input ==============
st.markdown('<div class="section-header">1. Từ khóa tìm kiếm</div>', unsafe_allow_html=True)

col_input, col_add = st.columns([4, 1])
with col_input:
    new_keyword = st.text_input(
        "Nhập từ khóa",
        placeholder="Ví dụ: rivaroxaban, aspirin, diabetes...",
        label_visibility="collapsed"
    )
with col_add:
    if st.button("➕ Thêm", use_container_width=True, type="primary"):
        if new_keyword.strip() and new_keyword.strip() not in st.session_state["keywords_list"]:
            st.session_state["keywords_list"].append(new_keyword.strip())
            st.rerun()

# Display keywords as tags
if st.session_state["keywords_list"]:
    cols = st.columns(min(len(st.session_state["keywords_list"]), 6))
    for idx, kw in enumerate(st.session_state["keywords_list"]):
        with cols[idx % 6]:
            col_tag, col_remove = st.columns([3, 1])
            with col_tag:
                st.markdown(f"**{kw}**")
            with col_remove:
                if st.button("✕", key=f"remove_{idx}", help="Xóa"):
                    st.session_state["keywords_list"].remove(kw)
                    st.rerun()
    
    if st.button("🗑️ Xóa tất cả", type="secondary"):
        st.session_state["keywords_list"] = []
        st.rerun()
else:
    st.info("💡 Thêm ít nhất 1 từ khóa để bắt đầu tìm kiếm")

# ============== SECTION 2: Filters ==============
st.markdown('<div class="section-header">2. Bộ lọc</div>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    date_mode = st.selectbox(
        "📅 Thời gian xuất bản",
        ["Không giới hạn", "1 năm", "5 năm", "10 năm", "Tùy chọn"],
        index=0
    )
    start_d = end_d = None
    if date_mode == "Tùy chọn":
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            start_d = st.date_input("Từ ngày", value=date(2020, 1, 1))
        with date_col2:
            end_d = st.date_input("Đến ngày", value=date.today())

with col2:
    st.markdown("**📄 Text availability**")
    tf_abstract = st.checkbox("Có abstract", value=False)
    tf_free = st.checkbox("Free full text")
    tf_full = st.checkbox("Full text")

with col3:
    st.markdown("**📑 Loại bài báo**")
    article_type_options = list(TERM_ARTICLE_TYPES.keys())
    selected_article_types = st.multiselect(
        "Chọn loại",
        options=article_type_options,
        default=[],
        label_visibility="collapsed"
    )

# Build filter lists
text_filters = [x for x, v in [("Abstract", tf_abstract), ("Free full text", tf_free), ("Full text", tf_full)] if v]
attr_filters = []
article_types = selected_article_types

# Map date_mode to internal values
date_mode_map = {
    "Không giới hạn": "None",
    "1 năm": "1 year",
    "5 năm": "5 years",
    "10 năm": "10 years",
    "Tùy chọn": "Custom range"
}
internal_date_mode = date_mode_map.get(date_mode, "None")

# ============== SECTION 3: (Advanced Settings removed as requested) ==============
# Sử dụng cấu hình mặc định cố định trong mã nguồn
email = "user@example.com"
api_key = None
max_records = 1000
batch_size = 200
rps = 2.8
within_group_mode = "OR"

# Output columns selection
st.markdown("**📊 Cột xuất ra**")
default_cols = ["pmid", "title", "authors", "journal", "pub_year", "doi", "abstract", "pubmed_url"]
all_cols = ["pmid", "title", "authors", "affiliations", "journal", "journal_iso", "pub_year", "pub_date", "doi", "publication_types", "languages", "keywords", "abstract", "pubmed_url"]
selected_cols = st.multiselect("Chọn cột hiển thị", options=all_cols, default=default_cols, label_visibility="collapsed")

# ============== SECTION 4: Actions ==============
st.markdown('<div class="section-header">3. Thực hiện</div>', unsafe_allow_html=True)

if not st.session_state["keywords_list"]:
    st.warning("⚠️ Vui lòng thêm từ khóa trước khi tìm kiếm")
    st.stop()

# Config (dùng các giá trị mặc định ở trên)
cfg = NcbiConfig(
    email=email,
    api_key=api_key,
    requests_per_second=float(rps),
)

mindate, maxdate = compute_date_range(internal_date_mode, start_d, end_d)

# Action buttons
col_preview, col_fetch = st.columns(2)
with col_preview:
    preview_clicked = st.button("🔍 Xem số lượng kết quả", use_container_width=True)
with col_fetch:
    run_clicked = st.button("🚀 Tải dữ liệu", use_container_width=True, type="primary")

# Preview counts
if preview_clicked:
    session = make_session()
    last_ts = 0.0
    counts = {}
    progress = st.progress(0, text="Đang kiểm tra...")
    for idx, kw in enumerate(st.session_state["keywords_list"]):
        eutils_term = build_eutils_query(kw, text_filters, attr_filters, article_types, within_group_mode=within_group_mode)
        last_ts = _throttle(last_ts, cfg.requests_per_second)
        try:
            c, _, _ = esearch_history(session, eutils_term, cfg, mindate=mindate, maxdate=maxdate)
            counts[kw] = c
        except Exception as e:
            counts[kw] = f"Error: {e}"
        progress.progress((idx + 1) / len(st.session_state["keywords_list"]))
    progress.empty()
    st.session_state["counts"] = counts

# Display counts
if st.session_state["counts"]:
    st.markdown("**Kết quả tìm kiếm:**")
    count_cols = st.columns(min(len(st.session_state["counts"]), 4))
    for idx, (kw, cnt) in enumerate(st.session_state["counts"].items()):
        with count_cols[idx % 4]:
            st.metric(label=kw, value=f"{cnt:,}" if isinstance(cnt, int) else cnt)

# Fetch data
if run_clicked:
    all_frames = []
    progress = st.progress(0, text="Đang tải dữ liệu...")
    
    for idx, kw in enumerate(st.session_state["keywords_list"]):
        eutils_term = build_eutils_query(kw, text_filters, attr_filters, article_types, within_group_mode=within_group_mode if 'within_group_mode' in dir() else "OR")
        try:
            df = fetch_pubmed(
                term=eutils_term,
                cfg=cfg,
                mindate=mindate,
                maxdate=maxdate,
                max_records=int(max_records) if 'max_records' in dir() else 1000,
                batch_size=int(batch_size) if 'batch_size' in dir() else 200,
            )
            if not df.empty:
                df.insert(0, "keyword", kw)
                all_frames.append(df)
        except Exception as e:
            st.error(f"Lỗi khi tải '{kw}': {e}")
        progress.progress((idx + 1) / len(st.session_state["keywords_list"]), text=f"Đã tải {idx + 1}/{len(st.session_state['keywords_list'])} từ khóa")
    
    progress.empty()
    
    df_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    
    # Keep selected columns
    keep = ["keyword"] + [c for c in selected_cols if c in df_all.columns]
    df_all = df_all[keep] if not df_all.empty else df_all
    
    # Deduplicate by PMID
    if "pmid" in df_all.columns and not df_all.empty:
        df_all = df_all.drop_duplicates(subset=["pmid"], keep="first").reset_index(drop=True)
    
    st.session_state["df_all"] = df_all
    st.success(f"✅ Đã tải xong {len(df_all)} bài báo!")

# ============== SECTION 5: Results & Export ==============
df_all = st.session_state.get("df_all", pd.DataFrame())

if df_all is not None and not df_all.empty:
    st.markdown('<div class="section-header">4. Kết quả & Xuất file</div>', unsafe_allow_html=True)
    
    # Stats
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1:
        st.metric("Tổng số bài", f"{len(df_all):,}")
    with stat_col2:
        unique_journals = df_all["journal"].nunique() if "journal" in df_all.columns else 0
        st.metric("Số tạp chí", f"{unique_journals:,}")
    with stat_col3:
        unique_keywords = df_all["keyword"].nunique() if "keyword" in df_all.columns else 0
        st.metric("Từ khóa", f"{unique_keywords:,}")
    
    # Data preview
    st.dataframe(
        df_all,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "pubmed_url": st.column_config.LinkColumn("PubMed URL", display_text="Xem"),
            "abstract": st.column_config.TextColumn("Abstract", width="large"),
        }
    )
    
    # Export buttons
    st.markdown("**📥 Tải xuống**")
    export_col1, export_col2, export_col3 = st.columns([1, 1, 2])
    
    with export_col1:
        csv_bytes = df_all.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Tải CSV",
            data=csv_bytes,
            file_name="pubmed_export.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with export_col2:
        try:
            from pandas import ExcelWriter
            xbuf = io.BytesIO()
            with ExcelWriter(xbuf, engine="openpyxl") as writer:
                df_all.to_excel(writer, index=False, sheet_name="PubMed_Data")
            st.download_button(
                "⬇️ Tải XLSX",
                data=xbuf.getvalue(),
                file_name="pubmed_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except Exception as e:
            st.warning(f"Không thể tạo XLSX: {e}")

# Footer
st.markdown("---")
# Ghi chú: API Key và các cài đặt nâng cao đã được ẩn theo yêu cầu
