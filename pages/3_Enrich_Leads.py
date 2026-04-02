import streamlit as st

from core.models import BusinessLead
from core.pipeline import enrich_leads
from utils.session import get_api_key, get_configured_providers

st.title("Enrich Leads")

# Check for leads
if "leads" not in st.session_state or not st.session_state["leads"]:
    st.warning("No leads to enrich. Go to **Scrape Leads** first.")
    st.stop()

leads_data = st.session_state["leads"]
st.info(f"**{len(leads_data)}** leads ready for enrichment from: *{st.session_state.get('leads_query', '')}*")

# --- Enrichment Options ---
st.subheader("Enrichment Providers")

email_providers = get_configured_providers("email")
social_providers = get_configured_providers("social")

if not email_providers and not social_providers:
    st.warning(
        "No enrichment providers configured. Go to **Setup API Keys** to add "
        "Hunter.io (emails) or Apollo.io (socials)."
    )
    st.stop()

selected_email = []
selected_social = []

if email_providers:
    st.markdown("**Email Enrichment**")
    provider_labels = {"hunter": "Hunter.io", "apollo": "Apollo.io"}
    for pid in email_providers:
        if st.checkbox(provider_labels.get(pid, pid), value=True, key=f"enrich_email_{pid}"):
            selected_email.append(pid)

if social_providers:
    st.markdown("**Social Media Enrichment**")
    provider_labels = {"apollo": "Apollo.io", "proxycurl": "Proxycurl"}
    for pid in social_providers:
        if st.checkbox(provider_labels.get(pid, pid), value=True, key=f"enrich_social_{pid}"):
            selected_social.append(pid)

# --- Run Enrichment ---
if st.button("Enrich Leads", type="primary", use_container_width=True):
    # Build enricher instances
    email_enrichers = []
    for pid in selected_email:
        key = get_api_key(pid)
        if not key:
            continue
        enricher = _create_enricher(pid, key, "email")
        if enricher:
            email_enrichers.append(enricher)

    social_enrichers = []
    for pid in selected_social:
        key = get_api_key(pid)
        if not key:
            continue
        enricher = _create_enricher(pid, key, "social")
        if enricher:
            social_enrichers.append(enricher)

    if not email_enrichers and not social_enrichers:
        st.error("No enrichers could be initialized.")
        st.stop()

    # Parse leads
    leads = [BusinessLead(**d) for d in leads_data]

    progress_bar = st.progress(0)
    status_text = st.empty()
    error_log = []

    def on_progress(current: int, total: int, message: str) -> None:
        if total > 0:
            progress_bar.progress(current / total)
        status_text.text(message)

    def on_error(idx: int, provider: str, exc: Exception) -> None:
        error_log.append(f"Lead {idx + 1} ({provider}): {exc}")

    try:
        enriched = enrich_leads(
            leads=leads,
            email_enrichers=email_enrichers,
            social_enrichers=social_enrichers,
            on_progress=on_progress,
            on_lead_error=on_error,
        )

        progress_bar.progress(1.0)

        # Update session state
        st.session_state["leads"] = [lead.model_dump() for lead in enriched]

        # Stats
        emails_found = sum(1 for l in enriched if l.business_email or l.personal_email)
        socials_found = sum(
            1 for l in enriched
            if l.company_linkedin or l.company_facebook or l.owner_linkedin
        )

        st.success(
            f"Enrichment complete! Found emails for **{emails_found}/{len(enriched)}** "
            f"leads and social profiles for **{socials_found}/{len(enriched)}** leads."
        )

        if error_log:
            with st.expander(f"Warnings ({len(error_log)})"):
                for err in error_log:
                    st.caption(err)

    except Exception as e:
        st.error(f"Enrichment failed: {e}")

# --- Display Results ---
if st.session_state.get("leads"):
    import pandas as pd

    st.markdown("---")
    st.subheader("Enriched Leads")

    df = pd.DataFrame(st.session_state["leads"])

    display_cols = [
        "business_name", "owner_name", "personal_email", "business_email",
        "phone", "website", "company_linkedin", "company_facebook",
        "company_instagram", "address",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

    st.info("Next: Go to **Export Results** to download your leads.")


def _create_enricher(pid: str, api_key: str, enricher_type: str):
    """Factory for enricher instances."""
    if pid == "hunter" and enricher_type == "email":
        from providers.hunter_client import HunterEnricher
        return HunterEnricher(api_key)
    elif pid == "apollo" and enricher_type == "email":
        from providers.apollo_client import ApolloEnricher
        return ApolloEnricher(api_key)
    elif pid == "apollo" and enricher_type == "social":
        from providers.apollo_client import ApolloEnricher
        return ApolloEnricher(api_key)
    return None
