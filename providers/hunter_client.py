"""Hunter.io email enrichment provider.

Endpoints used:
1. Discover (POST /v2/discover) — find contacts at a company by domain
2. Combined Enrichment (GET /v2/combined/find) — full person + company profile from email
3. Email Finder (GET /v2/email-finder) — find specific person's email by name + domain

Free tier: 50 credits/month, no credit card required.
API docs: https://hunter.io/api-documentation
"""

from typing import Dict, List, Optional, Set

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

        current = lead

        # Step 1: If we have an owner name, use Email Finder to get their email
        if current.owner_name and not current.personal_email:
            current = self._email_finder(current, domain)

        # Step 2: Use Discover to find contacts at the company
        if not current.personal_email:
            current = self._discover(current, domain)

        # Step 3: If we found an email, use Combined Enrichment for full profile
        email_to_enrich = current.personal_email or current.business_email
        if email_to_enrich:
            current = self._combined_enrichment(current, email_to_enrich)

        return current

    def _discover(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Discover endpoint to find contacts at a company."""
        self._rate.wait()

        try:
            resp = self._client.post(
                "/discover",
                params={"api_key": self._api_key},
                json={"domain": domain},
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

        # Find the best contact — prefer owners/founders
        best = self._pick_best_contact(emails)
        if not best:
            return lead

        updates = {"enriched_by": lead.enriched_by + ["hunter"]}

        email_addr = best.get("value")
        if email_addr:
            prefix = email_addr.split("@")[0].lower()
            if prefix in ("info", "contact", "hello", "office", "admin", "support", "sales"):
                if not lead.business_email:
                    updates["business_email"] = email_addr
            else:
                if not lead.personal_email:
                    updates["personal_email"] = email_addr

        # Capture name/title from discover results
        first = best.get("first_name", "")
        last = best.get("last_name", "")
        if not lead.owner_name and first:
            updates["owner_name"] = ("%s %s" % (first, last)).strip()
        if not lead.owner_title and best.get("position"):
            updates["owner_title"] = best["position"]

        return lead.model_copy(update=updates)

    def _email_finder(self, lead: BusinessLead, domain: str) -> BusinessLead:
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

        updates = {
            "personal_email": email,
            "email_confidence": data.get("score", 0),
            "enriched_by": lead.enriched_by + ["hunter"],
        }

        if not lead.owner_title and data.get("position"):
            updates["owner_title"] = data["position"]

        return lead.model_copy(update=updates)

    def _combined_enrichment(self, lead: BusinessLead, email: str) -> BusinessLead:
        """Use Combined Enrichment to get full person + company profile."""
        self._rate.wait()

        try:
            resp = self._client.get(
                "/combined/find",
                params={
                    "email": email,
                    "api_key": self._api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Hunter.io rate limit reached") from e
            return lead
        except Exception:
            return lead

        updates = {}
        if "hunter" not in lead.enriched_by:
            updates["enriched_by"] = lead.enriched_by + ["hunter"]

        # Person data
        person = data.get("person") or {}
        if not lead.owner_name and person.get("full_name"):
            updates["owner_name"] = person["full_name"]
        if not lead.owner_title and person.get("title"):
            updates["owner_title"] = person["title"]

        # Person socials
        if not lead.owner_linkedin and person.get("linkedin"):
            updates["owner_linkedin"] = person["linkedin"]
        if not lead.owner_twitter and person.get("twitter"):
            updates["owner_twitter"] = "https://twitter.com/%s" % person["twitter"]

        # Company data
        company = data.get("company") or {}
        if not lead.company_linkedin and company.get("linkedin"):
            updates["company_linkedin"] = company["linkedin"]
        if not lead.company_facebook and company.get("facebook"):
            updates["company_facebook"] = company["facebook"]
        if not lead.company_twitter and company.get("twitter"):
            updates["company_twitter"] = "https://twitter.com/%s" % company["twitter"]
        if not lead.company_instagram and company.get("instagram"):
            updates["company_instagram"] = company["instagram"]
        if not lead.company_youtube and company.get("youtube"):
            updates["company_youtube"] = company["youtube"]

        if updates:
            return lead.model_copy(update=updates)
        return lead

    @staticmethod
    def _pick_best_contact(emails: list) -> Optional[dict]:
        """Pick the best contact from Discover results — prefer owners/decision-makers."""
        if not emails:
            return None

        priority_titles = [
            "owner", "founder", "co-founder", "ceo", "president",
            "cto", "cfo", "coo", "chief", "vp", "vice president",
            "director", "manager",
        ]

        for title_kw in priority_titles:
            for entry in emails:
                position = (entry.get("position") or "").lower()
                if title_kw in position:
                    return entry

        # Fallback: highest confidence non-generic email
        personal = [
            e for e in emails
            if e.get("type") == "personal" or e.get("value", "").split("@")[0] not in
            ("info", "contact", "hello", "office", "admin", "support", "sales")
        ]
        if personal:
            return max(personal, key=lambda e: e.get("confidence", 0))

        # Last resort: any email
        return max(emails, key=lambda e: e.get("confidence", 0))
