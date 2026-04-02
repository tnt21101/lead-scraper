"""Apollo.io enrichment provider — emails + social profiles.

Uses two endpoints:
1. Organization Enrichment (/v1/organizations/enrich) — company socials by domain
2. People Match (/v1/people/match) — owner email/phone/socials by name + domain

Free tier: 75 credits/month, no credit card required.
API docs: https://docs.apollo.io/reference
"""

from datetime import datetime
from typing import Dict, Optional

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, SocialEnricher, ProgressCallback
from utils.rate_limiter import RateLimiter
from utils.validators import extract_domain

BASE_URL = "https://api.apollo.io/v1"


class ApolloEnricher(EmailEnricher, SocialEnricher):
    """Apollo.io serves as both email and social enricher."""

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

    @property
    def name(self) -> str:
        return "Apollo.io"

    def test_connection(self) -> bool:
        try:
            resp = self._client.get("/auth/health")
            return resp.status_code == 200
        except Exception:
            return False

    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        """Enrich a lead with company socials, then owner email/phone/socials."""
        domain = extract_domain(lead.website) if lead.website else None

        if not domain:
            return lead

        current = lead

        # Strategy 1: Organization Enrichment — gets company socials from domain
        # This works even without an owner name
        current = self._enrich_organization(current, domain)

        # Strategy 2: People Match — gets owner email/phone/socials
        # Only works if we have an owner name or can search by domain
        current = self._enrich_people(current, domain)

        return current

    def _enrich_organization(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Organization Enrichment to get company social profiles."""
        # Skip if we already have company socials
        if lead.company_linkedin and lead.company_facebook:
            return lead

        self._rate.wait()

        try:
            resp = self._client.get(
                "/organizations/enrich",
                params={"domain": domain},
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            if e.response.status_code in (401, 403):
                raise RuntimeError("Apollo.io API key invalid") from e
            return lead
        except Exception:
            return lead

        org = data.get("organization") or {}
        if not org:
            return lead

        updates = {"enriched_by": lead.enriched_by + ["apollo"]}

        # Company socials
        if not lead.company_linkedin and org.get("linkedin_url"):
            updates["company_linkedin"] = org["linkedin_url"]
        if not lead.company_facebook and org.get("facebook_url"):
            updates["company_facebook"] = org["facebook_url"]
        if not lead.company_twitter and org.get("twitter_url"):
            updates["company_twitter"] = org["twitter_url"]

        # Company email
        if not lead.business_email:
            primary = org.get("primary_email")
            if primary:
                updates["business_email"] = primary

        # Company phone
        if not lead.phone and org.get("phone"):
            updates["phone"] = org["phone"]

        return lead.model_copy(update=updates)

    def _enrich_people(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use People Match or People Search to find owner contact info."""
        # If we already have personal email and owner socials, skip
        if lead.personal_email and lead.owner_linkedin:
            return lead

        self._rate.wait()

        try:
            if lead.owner_name:
                # We have a name — use People Match (most accurate)
                payload = {"reveal_personal_emails": True, "domain": domain}
                parts = lead.owner_name.split(maxsplit=1)
                payload["first_name"] = parts[0]
                if len(parts) > 1:
                    payload["last_name"] = parts[1]
                payload["organization_name"] = lead.business_name

                resp = self._client.post("/people/match", json=payload)
            else:
                # No name — search for people at this company with senior titles
                payload = {
                    "organization_domains": [domain],
                    "person_seniorities": ["owner", "founder", "c_suite", "vp", "director", "manager"],
                    "page": 1,
                    "per_page": 1,
                }
                resp = self._client.post("/mixed_people/search", json=payload)

            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            if e.response.status_code in (401, 403):
                raise RuntimeError("Apollo.io API key invalid") from e
            return lead
        except Exception:
            return lead

        # Extract person from response (different structure per endpoint)
        person = data.get("person") or {}
        if not person:
            # mixed_people/search returns people in a list
            people = data.get("people") or []
            if people:
                person = people[0]

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
            if person.get("email_confidence"):
                updates["email_confidence"] = person["email_confidence"]

        # Phone
        if not lead.phone:
            phone_numbers = person.get("phone_numbers") or []
            if phone_numbers:
                updates["phone"] = phone_numbers[0].get("sanitized_number")

        # Owner socials
        if not lead.owner_linkedin and person.get("linkedin_url"):
            updates["owner_linkedin"] = person["linkedin_url"]
        if not lead.owner_facebook and person.get("facebook_url"):
            updates["owner_facebook"] = person["facebook_url"]
        if not lead.owner_twitter and person.get("twitter_url"):
            updates["owner_twitter"] = person["twitter_url"]

        if updates:
            return lead.model_copy(update=updates)
        return lead
