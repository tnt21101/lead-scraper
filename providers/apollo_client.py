"""Apollo.io enrichment provider — emails + social profiles.

Free plan endpoints used:
1. Organization Enrichment (/v1/organizations/enrich) — company socials by domain
2. Mixed People Search (/v1/mixed_people/search) — find people by company + seniority
3. Contacts Search (/v1/contacts/search) — search contacts database

NOTE: People Match (/v1/people/match) is NOT available on the free plan.

Free tier: 75 credits/month, no credit card required.
API docs: https://docs.apollo.io/reference
"""

from datetime import datetime
from typing import Dict, List, Optional

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, SocialEnricher, ProgressCallback
from utils.rate_limiter import RateLimiter
from utils.validators import extract_domain

BASE_URL = "https://api.apollo.io/api/v1"


class ApolloEnricher(EmailEnricher, SocialEnricher):
    """Apollo.io serves as both email and social enricher.

    Uses only free-plan endpoints: organizations/enrich,
    mixed_people/search, and contacts/search.
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

        # Step 1: Organization Enrichment — company socials from domain
        current = self._enrich_organization(current, domain)

        # Step 2: Mixed People Search — find owner/senior contacts at company
        current = self._enrich_people(current, domain)

        return current

    def _enrich_organization(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Organization Search to get company social profiles."""
        if lead.company_linkedin and lead.company_facebook:
            return lead

        self._rate.wait()

        try:
            resp = self._client.post(
                "/organizations/search",
                json={
                    "organization_domains": [domain],
                    "page": 1,
                    "per_page": 1,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            body = ""
            try:
                body = e.response.text[:200]
            except Exception:
                pass
            raise RuntimeError(
                "Apollo.io organizations/search returned %d: %s" % (e.response.status_code, body)
            ) from e
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError("Apollo.io organizations/search error: %s" % e) from e

        orgs = data.get("organizations") or []
        if not orgs:
            return lead

        org = orgs[0]

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

    def _enrich_people(self, lead: BusinessLead, domain: str) -> BusinessLead:
        """Use Organization Top People to find owner/senior contacts."""
        if lead.personal_email and lead.owner_linkedin:
            return lead

        self._rate.wait()

        try:
            payload = {
                "organization_domain": domain,
            }

            resp = self._client.post(
                "/mixed_people/organization_top_people", json=payload
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RuntimeError("Apollo.io rate limit reached") from e
            body = ""
            try:
                body = e.response.text[:200]
            except Exception:
                pass
            raise RuntimeError(
                "Apollo.io top_people returned %d: %s" % (e.response.status_code, body)
            ) from e
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError("Apollo.io top_people error: %s" % e) from e

        people = data.get("people") or data.get("top_people") or []
        if not people:
            return lead

        # Pick the best match — prefer owner/founder, then highest seniority
        person = self._pick_best_person(people, lead.owner_name)

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

    @staticmethod
    def _pick_best_person(people: list, owner_name: Optional[str] = None) -> dict:
        """Pick the most relevant person from a list of top people."""
        if not people:
            return {}

        # If we know the owner name, look for them first
        if owner_name:
            name_lower = owner_name.lower()
            for p in people:
                if name_lower in (p.get("name") or "").lower():
                    return p

        # Rank by seniority — prefer owners/founders
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

        # Fallback: return the first person
        return people[0]
