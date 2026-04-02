import streamlit as st

from utils.session import get_api_key, get_configured_providers

st.title("Scrape Leads")

# Check for configured maps providers
maps_providers = get_configured_providers("maps")

if not maps_providers:
    st.warning("No maps provider configured. Go to **Setup API Keys** to add one.")
    st.stop()

# --- Search Form ---
with st.form("search_form"):
    col1, col2 = st.columns([3, 1])

    with col1:
        query = st.text_input(
            "Search Query",
            placeholder="e.g. plumbers in Austin TX, coffee shops in San Francisco",
            help="Enter a business type and location, just like you would in Google Maps",
        )

    with col2:
        provider_labels = {
            "outscraper": "Outscraper (500 free/mo)",
            "serpapi": "SerpAPI (250 free/mo)",
            "scaleserp": "ScaleSERP (250 free/mo)",
        }
        provider = st.selectbox(
            "Provider",
            options=maps_providers,
            format_func=lambda x: provider_labels.get(x, x),
        )

    col3, col4 = st.columns([1, 1])
    with col3:
        limit = st.slider("Max results", min_value=5, max_value=100, value=20, step=5)
    with col4:
        st.markdown("")  # spacing

    submitted = st.form_submit_button("Search", type="primary", use_container_width=True)

# --- Execute Search ---
if submitted and query:
    api_key = get_api_key(provider)
    if not api_key:
        st.error(f"No API key found for {provider}")
        st.stop()

    scraper = _create_scraper(provider, api_key)
    if not scraper:
        st.error(f"Unknown provider: {provider}")
        st.stop()

    progress_bar = st.progress(0)
    status_text = st.empty()

    def on_progress(current: int, total: int, message: str) -> None:
        if total > 0:
            progress_bar.progress(current / total)
        status_text.text(message)

    try:
        with st.spinner(f"Scraping via {scraper.name}..."):
            leads = scraper.scrape(query=query, limit=limit, on_progress=on_progress)

        progress_bar.progress(1.0)
        status_text.text(f"Found {len(leads)} businesses")

        # Store in session state
        st.session_state["leads"] = [lead.model_dump() for lead in leads]
        st.session_state["leads_query"] = query

        st.success(f"Found **{len(leads)}** businesses for: *{query}*")

    except Exception as e:
        st.error(f"Scraping failed: {e}")

elif submitted and not query:
    st.warning("Please enter a search query.")

# --- Display Results ---
if "leads" in st.session_state and st.session_state["leads"]:
    import pandas as pd

    st.markdown("---")
    st.subheader(f"Results: {st.session_state.get('leads_query', '')}")

    df = pd.DataFrame(st.session_state["leads"])

    # Show key columns first
    display_cols = [
        "business_name", "phone", "website", "business_email",
        "address", "category", "rating", "reviews_count",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

    # Expandable full data view
    with st.expander("View all fields"):
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.info("Next: Go to **Enrich Leads** to add emails and social profiles.")


def _create_scraper(provider: str, api_key: str):
    """Factory for scraper instances."""
    if provider == "outscraper":
        from providers.outscraper_client import OutscraperScraper
        return OutscraperScraper(api_key)
    elif provider == "serpapi":
        from providers.serpapi_client import SerpAPIScraper
        return SerpAPIScraper(api_key)
    elif provider == "scaleserp":
        from providers.scaleserp_client import ScaleSERPScraper
        return ScaleSERPScraper(api_key)
    return None
