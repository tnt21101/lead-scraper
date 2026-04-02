import streamlit as st

from utils.session import API_KEY_CONFIG, get_api_key, set_api_key

st.title("Setup API Keys")
st.markdown(
    "Enter your API keys below. Keys are stored **only in your browser session** "
    "and are never saved to disk."
)

# Group providers by type
provider_groups = {
    "maps": ("Google Maps Scrapers", "Required to search for businesses"),
    "email": ("Email Enrichment", "Optional — adds email addresses to leads"),
    "social": ("Social Media Enrichment", "Optional — adds social profiles to leads"),
}

for ptype, (group_label, group_desc) in provider_groups.items():
    st.markdown(f"### {group_label}")
    st.caption(group_desc)

    providers = {k: v for k, v in API_KEY_CONFIG.items() if v["type"] == ptype}

    for pid, config in providers.items():
        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            current = get_api_key(pid) or ""
            key_val = st.text_input(
                config["label"],
                value=current,
                type="password",
                key=f"input_{pid}",
                help=f"{config['help']} — Free tier: {config['free_tier']}",
            )
            if key_val != current:
                set_api_key(pid, key_val)

        with col2:
            if get_api_key(pid):
                st.markdown("")  # spacing
                st.success("Configured", icon="✅")
            else:
                st.markdown("")
                st.caption("Not set")

        with col3:
            if get_api_key(pid):
                st.markdown("")
                if st.button("Test", key=f"test_{pid}"):
                    with st.spinner("Testing..."):
                        try:
                            provider_instance = _get_provider_instance(pid)
                            if provider_instance and provider_instance.test_connection():
                                st.success("Valid!")
                            else:
                                st.error("Invalid key")
                        except Exception as e:
                            st.error(f"Error: {e}")

    st.markdown("---")


def _get_provider_instance(pid: str):
    """Instantiate a provider for connection testing."""
    api_key = get_api_key(pid)
    if not api_key:
        return None

    if pid == "outscraper":
        from providers.outscraper_client import OutscraperScraper
        return OutscraperScraper(api_key)
    elif pid == "serpapi":
        from providers.serpapi_client import SerpAPIScraper
        return SerpAPIScraper(api_key)
    elif pid == "scaleserp":
        from providers.scaleserp_client import ScaleSERPScraper
        return ScaleSERPScraper(api_key)
    elif pid == "hunter":
        from providers.hunter_client import HunterEnricher
        return HunterEnricher(api_key)
    elif pid == "apollo":
        from providers.apollo_client import ApolloEnricher
        return ApolloEnricher(api_key)
    return None
