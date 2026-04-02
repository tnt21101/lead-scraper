"""Pipeline orchestrator — scrape and enrich in a single pass."""

from typing import Callable, List, Optional

from core.models import BusinessLead
from providers.base import EmailEnricher, MapsScraper, ProgressCallback, SocialEnricher
from providers.website_scraper import WebsiteScraper


# Shared website scraper instance (free, no API key needed)
_website_scraper = WebsiteScraper()


def scrape_and_enrich(
    query: str,
    limit: int,
    scraper: MapsScraper,
    email_enrichers: Optional[List[EmailEnricher]] = None,
    social_enrichers: Optional[List[SocialEnricher]] = None,
    on_progress: Optional[ProgressCallback] = None,
    on_lead_error: Optional[Callable[[int, str, Exception], None]] = None,
) -> List[BusinessLead]:
    """Full pipeline: scrape Google Maps, then enrich each lead.

    Enrichment order per lead:
    1. Website scraper (free) — scrapes the business website for emails/socials
    2. Email enrichers (Hunter, Apollo) — fills remaining email gaps
    3. Social enrichers (Apollo) — fills remaining social gaps
    """
    email_enrichers = email_enrichers or []
    social_enrichers = social_enrichers or []

    # Step 1: Scrape Google Maps
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

        # Step 2a: Website scraper (free) — always runs first
        try:
            current = _website_scraper.enrich(current)
        except Exception as e:
            if on_lead_error:
                on_lead_error(i, _website_scraper.name, e)

        # Step 2b: Email enrichment — only if website scraper didn't find emails
        if not current.business_email and not current.personal_email:
            for enricher in email_enrichers:
                try:
                    result = enricher.enrich(current)
                    if result.business_email or result.personal_email:
                        current = result
                        break
                except Exception as e:
                    if on_lead_error:
                        on_lead_error(i, enricher.name, e)

        # Step 2c: Social enrichment — only if website scraper didn't find socials
        has_socials = current.company_linkedin or current.company_facebook or current.company_instagram
        if not has_socials:
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
