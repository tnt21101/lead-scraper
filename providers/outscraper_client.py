"""Outscraper Google Maps scraper — primary provider.

Outscraper returns 45+ fields per business including social links, owner info,
and emails. Free tier: 500 businesses/month.

API docs: https://app.outscraper.com/api-docs
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import httpx

from core.models import BusinessLead
from providers.base import MapsScraper, ProgressCallback
from utils.rate_limiter import RateLimiter

BASE_URL = "https://api.app.outscraper.com"


class OutscraperScraper(MapsScraper):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={"X-API-KEY": api_key},
            timeout=60.0,
        )
        self._rate = RateLimiter(calls_per_second=1)

    @property
    def name(self) -> str:
        return "Outscraper"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get("/profile")
            return resp.status_code == 200
        except Exception:
            return False

    def scrape(
        self,
        query: str,
        limit: int = 20,
        on_progress: Optional[ProgressCallback] = None,
    ) -> List[BusinessLead]:
        if on_progress:
            on_progress(0, limit, "Submitting search request...")

        # Submit async search task
        self._rate.wait()
        resp = self._client.get(
            "/maps/search-v3",
            params={
                "query": query,
                "limit": limit,
                "async": "true",
            },
        )
        resp.raise_for_status()
        data = resp.json()

        request_id = data.get("id")
        if not request_id:
            # Synchronous response — results returned immediately
            return self._parse_results(data, on_progress, limit)

        # Poll for results
        return self._poll_results(request_id, limit, on_progress)

    def _poll_results(
        self,
        request_id: str,
        limit: int,
        on_progress: Optional[ProgressCallback],
    ) -> List[BusinessLead]:
        """Poll async task until results are ready."""
        max_wait = 120  # seconds
        poll_interval = 3
        elapsed = 0

        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            if on_progress:
                on_progress(0, limit, f"Waiting for results... ({elapsed}s)")

            self._rate.wait()
            resp = self._client.get(f"/requests/{request_id}")
            resp.raise_for_status()
            data = resp.json()

            status = data.get("status")
            if status == "Success":
                return self._parse_results(data, on_progress, limit)
            elif status in ("Error", "Failed"):
                raise RuntimeError(f"Outscraper task failed: {data.get('error', 'unknown')}")

        raise TimeoutError("Outscraper task timed out after 120 seconds")

    def _parse_results(
        self,
        data: dict,
        on_progress: Optional[ProgressCallback],
        limit: int,
    ) -> List[BusinessLead]:
        """Parse Outscraper response into BusinessLead objects."""
        results_list = data.get("data", data.get("results", []))

        # Outscraper returns nested lists — flatten
        items: List[Dict] = []
        for entry in results_list:
            if isinstance(entry, list):
                items.extend(entry)
            elif isinstance(entry, dict):
                items.append(entry)

        leads: List[BusinessLead] = []
        for i, item in enumerate(items[:limit]):
            if on_progress:
                on_progress(i + 1, min(len(items), limit), f"Parsing: {item.get('name', '?')}")

            lead = self._map_to_lead(item)
            leads.append(lead)

        return leads

    def _map_to_lead(self, item: dict) -> BusinessLead:
        """Map a single Outscraper result to a BusinessLead."""
        # Extract social links from the various fields Outscraper provides
        social = item.get("social_media", {}) if isinstance(item.get("social_media"), dict) else {}

        return BusinessLead(
            business_name=item.get("name", "Unknown"),
            address=item.get("full_address") or item.get("address"),
            phone=item.get("phone") or item.get("international_phone_number"),
            website=item.get("site") or item.get("website"),
            category=item.get("category") or item.get("type"),
            rating=_safe_float(item.get("rating")),
            reviews_count=_safe_int(item.get("reviews")),
            google_maps_url=item.get("google_maps_url") or item.get("link"),
            # Owner info
            owner_name=item.get("owner_name") or item.get("owner_title"),
            owner_title=item.get("owner_title"),
            # Emails
            business_email=_first(item.get("emails") or item.get("email")),
            # Social — company
            company_facebook=item.get("facebook") or social.get("facebook"),
            company_instagram=item.get("instagram") or social.get("instagram"),
            company_twitter=item.get("twitter") or social.get("twitter"),
            company_linkedin=item.get("linkedin") or social.get("linkedin"),
            company_youtube=item.get("youtube") or social.get("youtube"),
            # Metadata
            source_provider="outscraper",
            scraped_at=datetime.utcnow(),
        )


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _first(val) -> Optional[str]:
    """Extract first item from a value that may be a list or string."""
    if isinstance(val, list):
        return val[0] if val else None
    return val if val else None
