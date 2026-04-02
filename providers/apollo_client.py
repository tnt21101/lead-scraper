"""Apollo.io enrichment provider — emails + social profiles.

Free tier: 50 credits/month.
API docs: https://apolloio.github.io/apollo-api-docs/
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
    """Apollo.io serves as both email and social enricher.

    It returns emails, phone numbers, LinkedIn URLs, and other social
    profiles in a single API call.
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
        """Enrich a lead with email, phone, and social data from Apollo."""
        domain = extract_domain(lead.website) if lead.website else None

        if not domain and not lead.owner_name:
            return lead

        self._rate.wait()

        try:
            # Use people/match for best results
            payload: dict = {"reveal_personal_emails": True}

            if lead.owner_name:
                parts = lead.owner_name.split(maxsplit=1)
                payload["first_name"] = parts[0]
                if len(parts) > 1:
                    payload["last_name"] = parts[1]

            if domain:
                payload["organization_name"] = lead.business_name
                payload["domain"] = domain

            resp = self._client.post("/people/match", json=payload)
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

        person = data.get("person") or {}
        if not person:
            return lead

        org = person.get("organization") or {}

        updates: dict = {"enriched_by": lead.enriched_by + ["apollo"]}

        # Email
        if person.get("email") and not lead.personal_email:
            updates["personal_email"] = person["email"]
            updates["email_confidence"] = person.get("email_confidence")

        # Organization email
        if org.get("primary_email") and not lead.business_email:
            updates["business_email"] = org["primary_email"]

        # Owner info
        if not lead.owner_name and person.get("name"):
            updates["owner_name"] = person["name"]
        if not lead.owner_title and person.get("title"):
            updates["owner_title"] = person["title"]

        # Phone
        if not lead.phone:
            phone_numbers = person.get("phone_numbers") or []
            if phone_numbers:
                updates["phone"] = phone_numbers[0].get("sanitized_number")

        # Social — owner
        if not lead.owner_linkedin and person.get("linkedin_url"):
            updates["owner_linkedin"] = person["linkedin_url"]
        if not lead.owner_facebook and person.get("facebook_url"):
            updates["owner_facebook"] = person["facebook_url"]
        if not lead.owner_twitter and person.get("twitter_url"):
            updates["owner_twitter"] = person["twitter_url"]

        # Social — company
        if not lead.company_linkedin and org.get("linkedin_url"):
            updates["company_linkedin"] = org["linkedin_url"]
        if not lead.company_facebook and org.get("facebook_url"):
            updates["company_facebook"] = org["facebook_url"]
        if not lead.company_twitter and org.get("twitter_url"):
            updates["company_twitter"] = org["twitter_url"]

        return lead.model_copy(update=updates)
