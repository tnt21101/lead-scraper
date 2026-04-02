"""SerpAPI Google Maps scraper.

Free tier: 250 searches/month.
API docs: https://serpapi.com/google-maps-api
"""

from datetime import datetime
from typing import List, Optional

import httpx

from core.models import BusinessLead
from providers.base import MapsScraper, ProgressCallback
from utils.rate_limiter import RateLimiter

BASE_URL = "https://serpapi.com"


class SerpAPIScraper(MapsScraper):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(base_url=BASE_URL, timeout=30.0)
        self._rate = RateLimiter(calls_per_second=1)

    @property
    def name(self) -> str:
        return "SerpAPI"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get(
                "/search",
                params={
                    "engine": "google_maps",
                    "q": "test",
                    "api_key": self._api_key,
                    "num": 1,
                },
            )
            return resp.status_code == 200
        except Exception:
            return False

    def scrape(
        self,
        query: str,
        limit: int = 20,
        on_progress: Optional[ProgressCallback] = None,
    ) -> List[BusinessLead]:
        leads: List[BusinessLead] = []
        start = 0
        page_size = 20  # SerpAPI returns 20 per page

        while len(leads) < limit:
            if on_progress:
                on_progress(len(leads), limit, f"Fetching page {start // page_size + 1}...")

            self._rate.wait()
            resp = self._client.get(
                "/search",
                params={
                    "engine": "google_maps",
                    "q": query,
                    "api_key": self._api_key,
                    "start": start,
                    "num": min(page_size, limit - len(leads)),
                },
            )
            resp.raise_for_status()
            data = resp.json()

            results = data.get("local_results", [])
            if not results:
                break

            for item in results:
                if len(leads) >= limit:
                    break
                leads.append(self._map_to_lead(item))

                if on_progress:
                    on_progress(len(leads), limit, f"Parsing: {item.get('title', '?')}")

            # Check for next page
            if not data.get("serpapi_pagination", {}).get("next"):
                break
            start += page_size

        return leads

    def _map_to_lead(self, item: dict) -> BusinessLead:
        return BusinessLead(
            business_name=item.get("title", "Unknown"),
            address=item.get("address"),
            phone=item.get("phone"),
            website=item.get("website"),
            category=item.get("type"),
            rating=item.get("rating"),
            reviews_count=item.get("reviews"),
            google_maps_url=item.get("place_id_search"),
            source_provider="serpapi",
            scraped_at=datetime.utcnow(),
        )
