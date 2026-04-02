"""Hunter.io email enrichment provider.

Finds email addresses associated with a domain.
Free tier: 25 searches/month.

API docs: https://hunter.io/api-documentation
"""

from datetime import datetime
from typing import Dict, Optional

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, ProgressCallback
from utils.rate_limiter import RateLimiter
from utils.validators import extract_domain

BASE_URL = "https://api.hunter.io/v2"


class HunterEnricher(EmailEnricher):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(base_url=BASE_URL, timeout=30.0)
        self._rate = RateLimiter(calls_per_second=2)

    @property
    def name(self) -> str:
        return "Hunter.io"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get(
                "/account", params={"api_key": self._api_key}
            )
            return resp.status_code == 200
        except Exception:
            return False

    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        if not lead.website:
            return lead

        domain = extract_domain(lead.website)
        if not domain:
            return lead

        self._rate.wait()

        try:
            resp = self._client.get(
                "/domain-search",
                params={
                    "domain": domain,
                    "api_key": self._api_key,
                    "limit": 5,
                },
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Hunter.io rate limit reached") from e
            if e.response.status_code == 401:
                raise RuntimeError("Hunter.io API key invalid") from e
            return lead
        except Exception:
            return lead

        emails = data.get("emails", [])
        if not emails:
            return lead

        # Find the best email — prefer owner/founder roles
        owner_email = None
        generic_email = None
        best_confidence = 0

        for entry in emails:
            email_addr = entry.get("value")
            confidence = entry.get("confidence", 0)
            etype = entry.get("type", "")
            position = (entry.get("position") or "").lower()
            first_name = entry.get("first_name", "")
            last_name = entry.get("last_name", "")

            # Check if this looks like an owner/decision-maker
            is_owner = any(
                kw in position
                for kw in ("owner", "founder", "ceo", "president", "director", "manager")
            )

            if is_owner and confidence >= best_confidence:
                owner_email = email_addr
                best_confidence = confidence
                # Also capture the owner name if we don't have one
                if not lead.owner_name and first_name:
                    lead = lead.model_copy(
                        update={
                            "owner_name": f"{first_name} {last_name}".strip(),
                            "owner_title": entry.get("position"),
                        }
                    )

            if etype == "generic" and not generic_email:
                generic_email = email_addr

        updates: dict = {"enriched_by": lead.enriched_by + ["hunter"]}

        if owner_email:
            updates["personal_email"] = owner_email
            updates["email_confidence"] = best_confidence

        if generic_email and not lead.business_email:
            updates["business_email"] = generic_email

        # Fallback: use the highest-confidence email as business email
        if not updates.get("personal_email") and not lead.business_email and emails:
            top = max(emails, key=lambda e: e.get("confidence", 0))
            updates["business_email"] = top.get("value")
            updates["email_confidence"] = top.get("confidence")

        return lead.model_copy(update=updates)
