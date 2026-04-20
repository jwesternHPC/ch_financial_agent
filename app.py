import os
import streamlit as st
from ch_financial_agent import get_api_key, search_companies_by_name, run_analysis

# When running on Streamlit Community Cloud, secrets are stored in st.secrets
# rather than environment variables — copy the key across so get_api_key() finds it.
if "CH_API_KEY" in st.secrets:
    os.environ.setdefault("CH_API_KEY", st.secrets["CH_API_KEY"])

st.set_page_config(
    page_title="CH Financial Analyser",
    page_icon="📊",
    layout="centered",
)

st.title("Companies House Financial Analyser")
st.caption("Extract up to 10 years of financial data from UK Companies House filings.")

# ---------------------------------------------------------------------------
# API key — read from environment; fall back to sidebar input
# ---------------------------------------------------------------------------
try:
    api_key = get_api_key()
except EnvironmentError:
    with st.sidebar:
        st.header("Configuration")
        api_key = st.text_input(
            "Companies House API Key",
            type="password",
            help="Set the CH_API_KEY environment variable to avoid entering this each time.",
        )
    if not api_key:
        st.info("Enter your Companies House API key in the sidebar to get started.")
        st.stop()

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
defaults = {
    "company_number": None,
    "company_name": None,
    "search_results": None,
    "excel_bytes": None,
    "years_processed": 0,
}
for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ---------------------------------------------------------------------------
# Step 1 — Search
# ---------------------------------------------------------------------------
with st.form("search_form"):
    query = st.text_input(
        "Company name or number",
        placeholder="e.g. 08368703  or  Harwood Capital",
    )
    searched = st.form_submit_button("Search", type="primary")

if searched and query:
    # Reset any prior run when a new search is submitted
    st.session_state.company_number = None
    st.session_state.company_name = None
    st.session_state.search_results = None
    st.session_state.excel_bytes = None
    st.session_state.years_processed = 0

    query = query.strip()
    if query.isdigit():
        st.session_state.company_number = query.zfill(8)
    else:
        with st.spinner("Searching Companies House..."):
            results = search_companies_by_name(query, api_key)
        if results:
            st.session_state.search_results = results
        else:
            st.error(f"No companies found matching '{query}'.")

# ---------------------------------------------------------------------------
# Step 2 — Pick from search results (name search only)
# ---------------------------------------------------------------------------
if st.session_state.search_results and not st.session_state.company_number:
    st.write(f"**{len(st.session_state.search_results)} companies found — select one:**")
    for result in st.session_state.search_results:
        label = f"{result['name']}  ({result['number']})"
        if result.get("status"):
            label += f"  —  {result['status']}"
        if st.button(label, key=f"pick_{result['number']}"):
            st.session_state.company_number = result["number"]
            st.session_state.company_name = result["name"]
            st.session_state.search_results = None
            st.rerun()

# ---------------------------------------------------------------------------
# Step 3 — Run analysis
# ---------------------------------------------------------------------------
if st.session_state.company_number and not st.session_state.excel_bytes:
    display_name = st.session_state.company_name or st.session_state.company_number
    st.write(f"**Selected:** {display_name}")

    if st.button("Run Analysis", type="primary"):
        status_text = st.empty()
        progress_bar = st.progress(0)

        def on_progress(current, total, filing_date):
            progress_bar.progress(current / total)
            status_text.write(f"Processing filing {filing_date}  ({current} / {total})")

        try:
            excel_bytes, company_name, years = run_analysis(
                st.session_state.company_number,
                api_key,
                on_progress=on_progress,
            )
            if excel_bytes:
                st.session_state.excel_bytes = excel_bytes
                st.session_state.company_name = company_name
                st.session_state.years_processed = years
                progress_bar.progress(1.0)
                status_text.success(
                    f"Done — {years} year{'s' if years != 1 else ''} of filings processed for {company_name}."
                )
            else:
                progress_bar.empty()
                status_text.error("No financial data could be extracted from this company's filings.")
        except Exception as exc:
            progress_bar.empty()
            status_text.error(f"Analysis failed: {exc}")

# ---------------------------------------------------------------------------
# Step 4 — Download
# ---------------------------------------------------------------------------
if st.session_state.excel_bytes:
    filename = f"{st.session_state.company_number}_financial_analysis.xlsx"
    st.download_button(
        label="Download Excel",
        data=st.session_state.excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
