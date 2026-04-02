import streamlit as st

st.set_page_config(page_title="Lead Scraper", page_icon="🔍", layout="wide")


# ── Helpers (must be defined before use) ──────────────────────────────
def _create_scraper(pid, api_key):
    if pid == "outscraper":
        from providers.outscraper_client import OutscraperScraper
        return OutscraperScraper(api_key)
    elif pid == "serpapi":
        from providers.serpapi_client import SerpAPIScraper
        return SerpAPIScraper(api_key)
    elif pid == "scaleserp":
        from providers.scaleserp_client import ScaleSERPScraper
        return ScaleSERPScraper(api_key)
    return None


def _create_enricher(pid, api_key, enricher_type):
    if pid == "hunter" and enricher_type == "email":
        from providers.hunter_client import HunterEnricher
        return HunterEnricher(api_key)
    elif pid == "apollo":
        from providers.apollo_client import ApolloEnricher
        return ApolloEnricher(api_key)
    elif pid == "perplexity":
        from providers.perplexity_client import PerplexityEnricher
        return PerplexityEnricher(api_key)
    return None


def _test_provider(pid, api_key):
    """Test a provider's API key and return (success, message)."""
    try:
        instance = _create_scraper(pid, api_key) or _create_enricher(pid, api_key, "email") or _create_enricher(pid, api_key, "social")
        if instance and instance.test_connection():
            return True, "Connected"
        return False, "Invalid key"
    except Exception as e:
        return False, str(e)


# ── Free tier info per provider ───────────────────────────────────────
FREE_TIER_INFO = {
    "outscraper": (
        "Uses Google Maps Search API (/maps/search-v3). "
        "Free: 500 businesses/mo, no credit card required. "
        "Paid: ~$3 per 1,000 records (pay-as-you-go)."
    ),
    "serpapi": (
        "Uses Google Maps engine (engine=google_maps). "
        "Free: 250 searches/mo across all engines, no credit card required. "
        "Paid: $25/mo for 1,000 searches."
    ),
    "scaleserp": (
        "Uses Places search (search_type=places). "
        "Free: 125 searches/mo, no credit card required. "
        "Paid: $23/mo for 1,000 searches."
    ),
    "hunter": (
        "Uses Email Finder (/v2/email-finder) when owner name is known, "
        "falls back to Domain Search (/v2/domain-search). "
        "1 credit per lookup. Free: 50 credits/mo, no credit card required. "
        "Paid: $49/mo for 2,000 credits."
    ),
    "apollo": (
        "Uses People Match API (/v1/people/match). "
        "1 credit per email lookup, 8 credits per phone number. "
        "Free: 75 credits/mo (basic API access), no credit card required. "
        "Full enrichment requires Basic plan ($49/mo, 30,000 credits/yr)."
    ),
    "perplexity": (
        "Uses Sonar API to search the entire web for owner info. "
        "Best enricher for local businesses. ~$0.005-0.01 per query. "
        "No free tier — paid only. Get key at console.perplexity.ai."
    ),
}


# ── App UI ────────────────────────────────────────────────────────────
st.title("Lead Scraper")
st.caption("Search for businesses by type and location — get enriched leads with emails, socials, and contact info.")

from utils.session import API_KEY_CONFIG, get_api_key, set_api_key, get_configured_providers

# ── API Keys (collapsible) ────────────────────────────────────────────
with st.expander("API Keys", expanded=not get_configured_providers("maps")):
    st.caption("Enter your API keys below. Keys are stored only in your browser session.")

    groups = {
        "maps": "Google Maps Scraper (required)",
        "enrichment": "Email & Social Enrichment (optional)",
    }

    type_filters = {
        "maps": lambda t: t == "maps",
        "enrichment": lambda t: t in ("email", "social", "both"),
    }

    for ptype, group_label in groups.items():
        st.markdown("**%s**" % group_label)
        providers = {k: v for k, v in API_KEY_CONFIG.items() if type_filters[ptype](v["type"])}
        cols = st.columns(len(providers))
        for col, (pid, config) in zip(cols, providers.items()):
            with col:
                current = get_api_key(pid) or ""
                val = st.text_input(
                    config["label"],
                    value=current,
                    type="password",
                    key="key_%s" % pid,
                    help="%s — %s" % (FREE_TIER_INFO.get(pid, ""), config["help"]),
                )
                if val != current:
                    set_api_key(pid, val)

                # Status + Test button
                if get_api_key(pid):
                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.success("Set", icon="✅")
                    with c2:
                        if st.button("Test", key="test_%s" % pid):
                            with st.spinner("Testing..."):
                                ok, msg = _test_provider(pid, get_api_key(pid))
                                if ok:
                                    st.success(msg)
                                else:
                                    st.error(msg)

    st.markdown("---")
    st.info(
        "All services above offer free API access with **no credit card required**. "
        "Hover over the **?** icon next to each field for endpoint details, credit costs, and upgrade pricing.\n\n"
        "**Best free combo:** Outscraper (500 businesses/mo) + Hunter.io (50 email lookups/mo)"
    )

st.markdown("---")

# ── Search Form ───────────────────────────────────────────────────────
maps_providers = get_configured_providers("maps")

if not maps_providers:
    st.warning("Add at least one Google Maps API key above to get started.")
    st.stop()

provider_labels = {
    "outscraper": "Outscraper",
    "serpapi": "SerpAPI",
    "scaleserp": "ScaleSERP",
}

col1, col2, col3 = st.columns([3, 1, 1])

with col1:
    query = st.text_input(
        "Business type & location",
        placeholder="e.g. plumbers in Austin TX, coffee shops in San Francisco",
    )

with col2:
    provider = st.selectbox(
        "Maps provider",
        options=maps_providers,
        format_func=lambda x: provider_labels.get(x, x),
    )

with col3:
    limit = st.slider("Max results", min_value=5, max_value=100, value=20, step=5)

# Show which enrichment providers are active
email_providers = get_configured_providers("email")
social_providers = get_configured_providers("social")

status_parts = ["Website scraper (free, always active)"]
if email_providers:
    status_parts.append("Emails: %s" % ", ".join(email_providers))
if social_providers:
    status_parts.append("Socials: %s" % ", ".join(social_providers))
st.caption("Enrichment: %s" % " | ".join(status_parts))

# ── Run Pipeline ──────────────────────────────────────────────────────
if st.button("Search & Enrich", type="primary", use_container_width=True, disabled=not query):
    from core.pipeline import scrape_and_enrich

    scraper = _create_scraper(provider, get_api_key(provider))

    email_enrichers = []
    for pid in email_providers:
        e = _create_enricher(pid, get_api_key(pid), "email")
        if e:
            email_enrichers.append(e)

    social_enrichers = []
    for pid in social_providers:
        e = _create_enricher(pid, get_api_key(pid), "social")
        if e:
            social_enrichers.append(e)

    progress_bar = st.progress(0)
    status_text = st.empty()
    errors = []

    def on_progress(current, total, message):
        if total > 0:
            progress_bar.progress(min(current / total, 1.0))
        status_text.text(message)

    def on_error(idx, prov, exc):
        errors.append("Lead %d (%s): %s" % (idx + 1, prov, exc))

    try:
        leads = scrape_and_enrich(
            query=query,
            limit=limit,
            scraper=scraper,
            email_enrichers=email_enrichers,
            social_enrichers=social_enrichers,
            on_progress=on_progress,
            on_lead_error=on_error,
        )

        progress_bar.progress(1.0)
        status_text.empty()

        st.session_state["leads"] = [l.model_dump() for l in leads]
        st.session_state["leads_query"] = query

        emails_found = sum(1 for l in leads if l.business_email or l.personal_email)
        socials_found = sum(1 for l in leads if l.company_linkedin or l.company_facebook or l.owner_linkedin)
        st.success(
            "Found **%d** businesses — **%d** with emails, **%d** with social profiles"
            % (len(leads), emails_found, socials_found)
        )

        if errors:
            with st.expander("Warnings (%d)" % len(errors)):
                for err in errors:
                    st.caption(err)

    except Exception as e:
        st.error("Search failed: %s" % e)

# ── Results & Export ──────────────────────────────────────────────────
if st.session_state.get("leads"):
    import pandas as pd
    from core.export import export_csv, export_excel, COLUMN_LABELS, DEFAULT_COLUMNS

    leads_data = st.session_state["leads"]

    st.markdown("---")
    st.subheader("Results: %s" % st.session_state.get("leads_query", ""))

    df = pd.DataFrame(leads_data)

    display_cols = [c for c in DEFAULT_COLUMNS if c in df.columns and df[c].notna().any()]
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

    with st.expander("View all fields"):
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Export")
    safe_query = st.session_state.get("leads_query", "leads").replace(" ", "_")[:30]
    col_a, col_b = st.columns(2)

    with col_a:
        st.download_button(
            "Download CSV",
            data=export_csv(leads_data),
            file_name="%s_leads.csv" % safe_query,
            mime="text/csv",
            use_container_width=True,
        )
    with col_b:
        st.download_button(
            "Download Excel",
            data=export_excel(leads_data),
            file_name="%s_leads.xlsx" % safe_query,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
