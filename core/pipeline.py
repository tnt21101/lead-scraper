"""Pipeline orchestrator — chains scraping and enrichment layers."""

from typing import Callable, List, Optional

from core.models import BusinessLead
from providers.base import EmailEnricher, ProgressCallback, SocialEnricher


def enrich_leads(
    leads: List[BusinessLead],
    email_enrichers: Optional[List[EmailEnricher]] = None,
    social_enrichers: Optional[List[SocialEnricher]] = None,
    on_progress: Optional[ProgressCallback] = None,
    on_lead_error: Optional[Callable[[int, str, Exception], None]] = None,
) -> List[BusinessLead]:
    """Run enrichment pipeline on a list of leads.

    Enrichment failures are isolated per-lead — a single failure
    does not abort the entire batch.

    Args:
        leads: Leads to enrich.
        email_enrichers: Email providers to use (tried in order).
        social_enrichers: Social providers to use (tried in order).
        on_progress: Called with (current_index, total, message).
        on_lead_error: Called with (lead_index, provider_name, exception).

    Returns:
        Enriched leads (same order, same length as input).
    """
    email_enrichers = email_enrichers or []
    social_enrichers = social_enrichers or []
    total = len(leads)
    enriched: List[BusinessLead] = []

    for i, lead in enumerate(leads):
        if on_progress:
            on_progress(i, total, f"Enriching: {lead.business_name}")

        current = lead

        # Email enrichment — try each provider, stop on first success
        for enricher in email_enrichers:
            try:
                result = enricher.enrich(current)
                if result.business_email or result.personal_email:
                    current = result
                    break  # Got emails, no need to try more
            except Exception as e:
                if on_lead_error:
                    on_lead_error(i, enricher.name, e)

        # Social enrichment — try each provider
        for enricher in social_enrichers:
            try:
                result = enricher.enrich(current)
                current = result
            except Exception as e:
                if on_lead_error:
                    on_lead_error(i, enricher.name, e)

        enriched.append(current)

    if on_progress:
        on_progress(total, total, "Enrichment complete")

    return enriched
