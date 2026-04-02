"""Pipeline orchestrator — scrape and enrich in a single pass."""

from typing import Callable, List, Optional

from core.models import BusinessLead
from providers.base import EmailEnricher, MapsScraper, ProgressCallback, SocialEnricher


def scrape_and_enrich(
    query: str,
    limit: int,
    scraper: MapsScraper,
    email_enrichers: Optional[List[EmailEnricher]] = None,
    social_enrichers: Optional[List[SocialEnricher]] = None,
    on_progress: Optional[ProgressCallback] = None,
    on_lead_error: Optional[Callable[[int, str, Exception], None]] = None,
) -> List[BusinessLead]:
    """Full pipeline: scrape Google Maps then auto-enrich all leads.

    Args:
        query: Search query (e.g. "plumbers in Austin TX").
        limit: Max number of leads to scrape.
        scraper: Google Maps scraper instance.
        email_enrichers: Email providers (tried in order per lead).
        social_enrichers: Social providers (tried in order per lead).
        on_progress: Called with (current, total, message).
        on_lead_error: Called with (lead_index, provider_name, exception).

    Returns:
        Fully enriched leads list.
    """
    email_enrichers = email_enrichers or []
    social_enrichers = social_enrichers or []

    # Step 1: Scrape
    if on_progress:
        on_progress(0, limit, "Scraping Google Maps...")

    leads = scraper.scrape(query=query, limit=limit, on_progress=on_progress)

    if not leads:
        return []

    # Step 2: Enrich each lead
    total = len(leads)
    enriched: List[BusinessLead] = []

    for i, lead in enumerate(leads):
        if on_progress:
            on_progress(i, total, "Enriching: %s" % lead.business_name)

        current = lead

        # Email enrichment — try each provider, stop on first success
        for enricher in email_enrichers:
            try:
                result = enricher.enrich(current)
                if result.business_email or result.personal_email:
                    current = result
                    break
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
        on_progress(total, total, "Done — %d leads enriched" % total)

    return enriched
