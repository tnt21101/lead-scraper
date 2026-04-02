"""ScaleSERP Google Maps scraper (by Traject Data).

Free tier: ~250 searches/month.
API docs: https://docs.scaleserp.com/
"""

from datetime import datetime
from typing import List, Optional

import httpx

from core.models import BusinessLead
from providers.base import MapsScraper, ProgressCallback
from utils.rate_limiter import RateLimiter

BASE_URL = "https://api.scaleserp.com"


class ScaleSERPScraper(MapsScraper):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(base_url=BASE_URL, timeout=30.0)
        self._rate = RateLimiter(calls_per_second=1)

    @property
    def name(self) -> str:
        return "ScaleSERP"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get(
                "/account",
                params={"api_key": self._api_key},
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
        page = 1

        while len(leads) < limit:
            if on_progress:
                on_progress(len(leads), limit, f"Fetching page {page}...")

            self._rate.wait()
            resp = self._client.get(
                "/search",
                params={
                    "api_key": self._api_key,
                    "search_type": "maps",
                    "q": query,
                    "page": page,
                    "num": min(20, limit - len(leads)),
                },
            )
            resp.raise_for_status()
            data = resp.json()

            results = data.get("maps_results", data.get("local_results", []))
            if not results:
                break

            for item in results:
                if len(leads) >= limit:
                    break
                leads.append(self._map_to_lead(item))

                if on_progress:
                    on_progress(len(leads), limit, f"Parsing: {item.get('title', '?')}")

            page += 1

            # No more pages
            if len(results) < 20:
                break

        return leads

    def _map_to_lead(self, item: dict) -> BusinessLead:
        return BusinessLead(
            business_name=item.get("title", "Unknown"),
            address=item.get("address"),
            phone=item.get("phone"),
            website=item.get("website") or item.get("link"),
            category=item.get("type"),
            rating=item.get("rating"),
            reviews_count=item.get("reviews"),
            google_maps_url=item.get("data_cid") or item.get("link"),
            source_provider="scaleserp",
            scraped_at=datetime.utcnow(),
        )
