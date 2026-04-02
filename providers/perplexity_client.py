"""Perplexity Sonar API — web-powered lead enrichment.

Uses Perplexity's AI search to find business owner information
by searching the entire web. Most effective enricher for small
local businesses that aren't in B2B databases.

Pricing: ~$0.005-0.01 per query (no free tier).
API docs: https://docs.perplexity.ai/
"""

from typing import Optional

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, SocialEnricher, ProgressCallback
from utils.rate_limiter import RateLimiter

BASE_URL = "https://api.perplexity.ai"

ENRICHMENT_PROMPT = """Find the following information about this business. Return ONLY a JSON object with these exact keys, no other text:

Business: {business_name}
Location: {address}
Website: {website}

Return this JSON:
{{
  "owner_name": "full name of the business owner or primary contact, or null",
  "owner_title": "their title (Owner, CEO, Founder, etc), or null",
  "personal_email": "their personal or direct email, or null",
  "business_email": "general business email (info@, contact@, etc), or null",
  "phone": "business phone number, or null",
  "owner_linkedin": "owner's LinkedIn profile URL, or null",
  "owner_facebook": "owner's personal Facebook URL, or null",
  "owner_twitter": "owner's Twitter/X URL, or null",
  "company_linkedin": "company LinkedIn page URL, or null",
  "company_facebook": "company Facebook page URL, or null",
  "company_instagram": "company Instagram URL, or null",
  "company_twitter": "company Twitter/X URL, or null"
}}

Only include data you can verify from web sources. Use null for anything you can't find."""


class PerplexityEnricher(EmailEnricher, SocialEnricher):
    """Perplexity Sonar API — searches the entire web for lead data."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={
                "Authorization": "Bearer %s" % api_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._rate = RateLimiter(calls_per_second=0.8)  # ~50 RPM on free tier

    @property
    def name(self) -> str:
        return "Perplexity"

    def test_connection(self) -> bool:
        try:
            resp = self._client.post(
                "/v1/sonar",
                json={
                    "model": "sonar",
                    "messages": [{"role": "user", "content": "test"}],
                },
            )
            return resp.status_code == 200
        except Exception:
            return False

    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        if not lead.business_name:
            return lead

        self._rate.wait()

        prompt = ENRICHMENT_PROMPT.format(
            business_name=lead.business_name,
            address=lead.address or "unknown",
            website=lead.website or "unknown",
        )

        try:
            resp = self._client.post(
                "/v1/sonar",
                json={
                    "model": "sonar",
                    "messages": [{"role": "user", "content": prompt}],
                    "web_search_options": {
                        "search_context_size": "medium",
                    },
                },
            )
            if resp.status_code == 429:
                raise RuntimeError("Perplexity rate limit reached")
            if resp.status_code in (401, 403):
                raise RuntimeError("Perplexity API key invalid")
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            body = ""
            try:
                body = e.response.text[:200]
            except Exception:
                pass
            raise RuntimeError(
                "Perplexity returned %d: %s" % (e.response.status_code, body)
            ) from e
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError("Perplexity error: %s" % e) from e

        # Extract the AI response
        content = ""
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError):
            return lead

        # Parse JSON from response
        parsed = self._parse_json(content)
        if not parsed:
            return lead

        return self._apply_data(lead, parsed)

    def _parse_json(self, text: str) -> Optional[dict]:
        """Extract JSON from the AI response text."""
        import json

        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try extracting JSON from markdown code block
        import re
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding JSON object in the text
        match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        return None

    def _apply_data(self, lead: BusinessLead, data: dict) -> BusinessLead:
        """Apply parsed Perplexity data to the lead."""
        updates = {"enriched_by": lead.enriched_by + ["perplexity"]}

        field_map = {
            "owner_name": "owner_name",
            "owner_title": "owner_title",
            "personal_email": "personal_email",
            "business_email": "business_email",
            "phone": "phone",
            "owner_linkedin": "owner_linkedin",
            "owner_facebook": "owner_facebook",
            "owner_twitter": "owner_twitter",
            "company_linkedin": "company_linkedin",
            "company_facebook": "company_facebook",
            "company_instagram": "company_instagram",
            "company_twitter": "company_twitter",
        }

        for json_key, lead_field in field_map.items():
            value = data.get(json_key)
            if value and value != "null" and not getattr(lead, lead_field, None):
                updates[lead_field] = value

        return lead.model_copy(update=updates)
