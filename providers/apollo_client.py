"""Apollo.io enrichment provider — emails + social profiles.

Verified endpoints from official docs (https://docs.apollo.io/reference):
1. People Enrichment: POST /api/v1/people/match
2. People API Search: POST /api/v1/mixed_people/api_search
3. Organization Enrichment: GET /api/v1/organizations/enrich?domain=X

Free tier: 75 credits/month, no credit card required.
"""

from datetime import datetime
from typing import Dict, List, Optional

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, SocialEnricher, ProgressCallback
from utils.rate_limiter import RateLimiter
from utils.validators import extract_domain

BASE_URL = "https://api.apollo.io"


class ApolloEnricher(EmailEnricher, SocialEnricher):
    """Apollo.io serves as both email and social enricher.

    Tries endpoints in order, skipping any that return 403.
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key,
            },
            timeout=30.0,
        )
        self._rate = RateLimiter(calls_per_second=1)
        # Track which endpoints are blocked so we don't retry them
        self._blocked_endpoints = set()

    @property
    def name(self) -> str:
        return "Apollo.io"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get("/v1/auth/health")
            return resp.status_code == 200
        except Exception:
            return False

    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        domain = extract_domain(lead.website) if lead.website else None
        if not domain:
            return lead

        current = lead

        # Try Organization Enrichment for company socials
        if "org_enrich" not in self._blocked_endpoints:
            current = self._org_enrich(current, domain)

        # Try People Enrichment if we have a name
        if current.owner_name and "people_match" not in self._blocked_endpoints:
            current = self._people_match(current, domain)

        # Try People API Search as fallback
        if (not current.personal_email or not current.owner_name) and "people_search" not in self._blocked_endpoints:
            current = self._people_search(current, domain)

        return current

    # ── Organization Enrichment ───────────────────────────────────────
    # GET /api/v1/organizations/enrich?domain=X
    def _org_enrich(self, lead: BusinessLead, domain: str) -> BusinessLead:
        if lead.company_linkedin and lead.company_facebook:
            return lead

        self._rate.wait()
        try:
            resp = self._client.get(
                "/api/v1/organizations/enrich",
                params={"domain": domain},
            )
            if resp.status_code == 403:
                self._blocked_endpoints.add("org_enrich")
                return lead
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            return lead
        except Exception:
            return lead

        org = data.get("organization") or {}
        if not org:
            return lead

        updates = {"enriched_by": lead.enriched_by + ["apollo"]}
        if not lead.company_linkedin and org.get("linkedin_url"):
            updates["company_linkedin"] = org["linkedin_url"]
        if not lead.company_facebook and org.get("facebook_url"):
            updates["company_facebook"] = org["facebook_url"]
        if not lead.company_twitter and org.get("twitter_url"):
            updates["company_twitter"] = org["twitter_url"]
        if not lead.business_email and org.get("primary_email"):
            updates["business_email"] = org["primary_email"]
        if not lead.phone and org.get("phone"):
            updates["phone"] = org["phone"]

        return lead.model_copy(update=updates)

    # ── People Enrichment ─────────────────────────────────────────────
    # POST /api/v1/people/match
    def _people_match(self, lead: BusinessLead, domain: str) -> BusinessLead:
        if not lead.owner_name:
            return lead

        self._rate.wait()
        parts = lead.owner_name.split(maxsplit=1)
        payload = {
            "first_name": parts[0],
            "domain": domain,
            "reveal_personal_emails": True,
        }
        if len(parts) > 1:
            payload["last_name"] = parts[1]

        try:
            resp = self._client.post("/api/v1/people/match", json=payload)
            if resp.status_code == 403:
                self._blocked_endpoints.add("people_match")
                return lead
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            return lead
        except Exception:
            return lead

        person = data.get("person") or {}
        return self._apply_person_data(lead, person)

    # ── People API Search ─────────────────────────────────────────────
    # POST /api/v1/mixed_people/api_search
    def _people_search(self, lead: BusinessLead, domain: str) -> BusinessLead:
        self._rate.wait()

        payload = {
            "q_organization_domains_list": [domain],
            "person_seniorities": [
                "owner", "founder", "c_suite", "partner", "vp", "head",
                "director", "manager",
            ],
            "page": 1,
            "per_page": 1,
        }

        try:
            resp = self._client.post(
                "/api/v1/mixed_people/api_search", json=payload
            )
            if resp.status_code == 403:
                self._blocked_endpoints.add("people_search")
                return lead
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            return lead
        except Exception:
            return lead

        people = data.get("people") or []
        if not people:
            return lead

        person = self._pick_best_person(people, lead.owner_name)
        return self._apply_person_data(lead, person)

    # ── Shared helpers ────────────────────────────────────────────────
    def _apply_person_data(self, lead: BusinessLead, person: dict) -> BusinessLead:
        if not person:
            return lead

        updates = {}
        if "apollo" not in lead.enriched_by:
            updates["enriched_by"] = lead.enriched_by + ["apollo"]

        # Owner info
        if not lead.owner_name and person.get("name"):
            updates["owner_name"] = person["name"]
        if not lead.owner_title and person.get("title"):
            updates["owner_title"] = person["title"]

        # Email
        if not lead.personal_email and person.get("email"):
            updates["personal_email"] = person["email"]
            if person.get("extrapolated_email_confidence"):
                updates["email_confidence"] = person["extrapolated_email_confidence"]

        # Phone
        if not lead.phone:
            contact = person.get("contact") or {}
            phone_numbers = contact.get("phone_numbers") or person.get("phone_numbers") or []
            if phone_numbers:
                updates["phone"] = phone_numbers[0].get("sanitized_number")

        # Owner socials
        if not lead.owner_linkedin and person.get("linkedin_url"):
            updates["owner_linkedin"] = person["linkedin_url"]
        if not lead.owner_facebook and person.get("facebook_url"):
            updates["owner_facebook"] = person["facebook_url"]
        if not lead.owner_twitter and person.get("twitter_url"):
            updates["owner_twitter"] = person["twitter_url"]

        # Company socials from person's organization data
        org = person.get("organization") or {}
        if not lead.company_linkedin and org.get("linkedin_url"):
            updates["company_linkedin"] = org["linkedin_url"]
        if not lead.company_facebook and org.get("facebook_url"):
            updates["company_facebook"] = org["facebook_url"]
        if not lead.company_twitter and org.get("twitter_url"):
            updates["company_twitter"] = org["twitter_url"]
        if not lead.business_email and org.get("primary_email"):
            updates["business_email"] = org["primary_email"]

        if updates:
            return lead.model_copy(update=updates)
        return lead

    @staticmethod
    def _pick_best_person(people: list, owner_name: Optional[str] = None) -> dict:
        if not people:
            return {}

        if owner_name:
            name_lower = owner_name.lower()
            for p in people:
                if name_lower in (p.get("name") or "").lower():
                    return p

        priority_titles = [
            "owner", "founder", "co-founder", "ceo", "president",
            "cto", "cfo", "coo", "chief", "vp", "vice president",
            "director", "manager",
        ]
        for title_kw in priority_titles:
            for p in people:
                person_title = (p.get("title") or "").lower()
                if title_kw in person_title:
                    return p

        return people[0]
