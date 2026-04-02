import streamlit as st

st.set_page_config(
    page_title="Lead Scraper",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Lead Scraper")
st.markdown(
    "Search Google Maps for businesses, enrich with emails & social profiles, "
    "and export your leads."
)

st.markdown("---")

st.markdown(
    """
### Getting Started

1. **Setup API Keys** — Add your API keys for the services you want to use
2. **Scrape Leads** — Search Google Maps for businesses in any location
3. **Enrich Leads** — Add emails, phone numbers, and social profiles
4. **Export Results** — Download your leads as CSV or Excel

Use the sidebar to navigate between pages.
"""
)

# Show quick status in sidebar
with st.sidebar:
    st.markdown("### Status")
    from utils.session import get_configured_providers

    maps = get_configured_providers("maps")
    email = get_configured_providers("email")
    social = get_configured_providers("social")

    if maps:
        st.success(f"Maps: {', '.join(maps)}")
    else:
        st.warning("No maps provider configured")

    if email:
        st.success(f"Email: {', '.join(email)}")
    else:
        st.info("No email provider configured")

    if social:
        st.success(f"Social: {', '.join(social)}")
    else:
        st.info("No social provider configured")

    leads = st.session_state.get("leads", [])
    if leads:
        st.metric("Leads scraped", len(leads))
