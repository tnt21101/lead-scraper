"""Hunter.io email enrichment provider.

Uses two endpoints:
1. Email Finder (/v2/email-finder) — finds a specific person's email given name + domain
2. Domain Search (/v2/domain-search) — lists all emails found for a domain

Free tier: 50 credits/month, no credit card required.
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

        # Strategy 1: If we have an owner name, use Email Finder (most accurate)
        if lead.owner_name:
            result = self._try_email_finder(lead, domain)
            if result.personal_email or result.business_email:
                return result

        # Strategy 2: Domain Search to find all emails for this domain
        result = self._try_domain_search(lead, domain)
        return result

    def _try_email_finder(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Email Finder to locate a specific person's email."""
        parts = lead.owner_name.split(maxsplit=1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

        if not first_name:
            return lead

        self._rate.wait()

        try:
            params = {
                "domain": domain,
                "first_name": first_name,
                "api_key": self._api_key,
            }
            if last_name:
                params["last_name"] = last_name

            resp = self._client.get("/email-finder", params=params)
            resp.raise_for_status()
            data = resp.json().get("data", {})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Hunter.io rate limit reached") from e
            return lead
        except Exception:
            return lead

        email = data.get("email")
        if not email:
            return lead

        confidence = data.get("score", 0)
        updates = {
            "personal_email": email,
            "email_confidence": confidence,
            "enriched_by": lead.enriched_by + ["hunter"],
        }

        # Capture name details if we didn't have them
        if not lead.owner_name:
            fn = data.get("first_name", "")
            ln = data.get("last_name", "")
            if fn:
                updates["owner_name"] = ("%s %s" % (fn, ln)).strip()

        if not lead.owner_title and data.get("position"):
            updates["owner_title"] = data["position"]

        return lead.model_copy(update=updates)

    def _try_domain_search(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Domain Search to find all emails for a domain."""
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

            is_owner = any(
                kw in position
                for kw in ("owner", "founder", "ceo", "president", "director", "manager")
            )

            if is_owner and confidence >= best_confidence:
                owner_email = email_addr
                best_confidence = confidence
                if not lead.owner_name and first_name:
                    lead = lead.model_copy(
                        update={
                            "owner_name": ("%s %s" % (first_name, last_name)).strip(),
                            "owner_title": entry.get("position"),
                        }
                    )

            if etype == "generic" and not generic_email:
                generic_email = email_addr

        updates = {"enriched_by": lead.enriched_by + ["hunter"]}

        if owner_email:
            updates["personal_email"] = owner_email
            updates["email_confidence"] = best_confidence

        if generic_email and not lead.business_email:
            updates["business_email"] = generic_email

        # Fallback: use highest-confidence email
        if not updates.get("personal_email") and not lead.business_email and emails:
            top = max(emails, key=lambda e: e.get("confidence", 0))
            updates["business_email"] = top.get("value")
            updates["email_confidence"] = top.get("confidence")

        return lead.model_copy(update=updates)
