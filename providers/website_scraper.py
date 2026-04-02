"""Free website scraper — extracts emails, social links, and owner info
directly from a business's website. No API key needed.

Scrapes the homepage + /contact + /about pages looking for:
- Email addresses (mailto: links, email patterns in text)
- Social media URLs (LinkedIn, Facebook, Instagram, Twitter, YouTube)
- Owner/team names from About page (best effort)
"""

import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx

from core.models import BusinessLead
from providers.base import EmailEnricher, SocialEnricher, ProgressCallback

# Paths to check on each business website
PAGES_TO_SCRAPE = ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]

# Email regex — matches standard email patterns
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

# Social media URL patterns
SOCIAL_PATTERNS = {
    "facebook": re.compile(r"https?://(?:www\.)?facebook\.com/[a-zA-Z0-9._\-/]+", re.I),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[a-zA-Z0-9._\-/]+", re.I),
    "twitter": re.compile(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[a-zA-Z0-9._\-/]+", re.I),
    "linkedin": re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9._\-/]+", re.I),
    "youtube": re.compile(r"https?://(?:www\.)?youtube\.com/(?:channel|c|@)[a-zA-Z0-9._\-/]+", re.I),
}

# Junk email patterns to skip
JUNK_EMAILS = {
    "sentry.io", "wixpress.com", "example.com", "email.com",
    "yourdomain.com", "domain.com", "yoursite.com",
}

# Common generic email prefixes (still useful but lower priority than personal)
GENERIC_PREFIXES = {"info", "contact", "hello", "office", "admin", "support", "sales", "help"}


class WebsiteScraper(EmailEnricher, SocialEnricher):
    """Scrapes business websites for emails, social links, and owner info.
    Free — no API key required.
    """

    def __init__(self) -> None:
        self._client = httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)",
            },
        )

    @property
    def name(self) -> str:
        return "Website Scraper"

    def test_connection(self) -> bool:
        return True  # No API key needed

    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        if not lead.website:
            return lead

        base_url = lead.website
        if not base_url.startswith("http"):
            base_url = "https://%s" % base_url

        all_emails: Set[str] = set()
        all_socials: Dict[str, str] = {}
        page_text = ""

        for path in PAGES_TO_SCRAPE:
            url = urljoin(base_url, path) if path else base_url
            try:
                resp = self._client.get(url)
                if resp.status_code != 200:
                    continue
                html = resp.text

                # Extract emails
                emails = self._extract_emails(html, lead.website)
                all_emails.update(emails)

                # Extract social links
                socials = self._extract_socials(html)
                for platform, url_found in socials.items():
                    if platform not in all_socials:
                        all_socials[platform] = url_found

                # Save text from about page for owner name extraction
                if "about" in path:
                    page_text = html

            except Exception:
                continue

        if not all_emails and not all_socials:
            return lead

        updates = {"enriched_by": lead.enriched_by + ["website"]}

        # Assign emails — personal (non-generic) vs business (generic)
        personal, business = self._categorize_emails(all_emails)
        if not lead.personal_email and personal:
            updates["personal_email"] = personal
        if not lead.business_email and business:
            updates["business_email"] = business
        # If we only found one type, use it for whichever is missing
        if not lead.business_email and not updates.get("business_email") and personal:
            updates["business_email"] = personal
        if not lead.personal_email and not updates.get("personal_email") and business:
            updates["personal_email"] = business

        # Assign social links
        if not lead.company_facebook and "facebook" in all_socials:
            updates["company_facebook"] = all_socials["facebook"]
        if not lead.company_instagram and "instagram" in all_socials:
            updates["company_instagram"] = all_socials["instagram"]
        if not lead.company_twitter and "twitter" in all_socials:
            updates["company_twitter"] = all_socials["twitter"]
        if not lead.company_linkedin and "linkedin" in all_socials:
            lnk = all_socials["linkedin"]
            if "/company/" in lnk:
                updates["company_linkedin"] = lnk
            elif "/in/" in lnk and not lead.owner_linkedin:
                updates["owner_linkedin"] = lnk
        if not lead.company_youtube and "youtube" in all_socials:
            updates["company_youtube"] = all_socials["youtube"]

        return lead.model_copy(update=updates)

    def _extract_emails(self, html: str, website: str) -> Set[str]:
        """Find email addresses in HTML content."""
        # Strip HTML tags to reduce false positives
        import re as _re
        text = _re.sub(r"<[^>]+>", " ", html)
        # Decode HTML entities
        text = text.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&")
        text = text.replace("\\u003e", ">").replace("\\u003c", "<")
        text = _re.sub(r"&#?\w+;", " ", text)
        text = _re.sub(r"\\u[0-9a-fA-F]{4}", " ", text)

        found = set()
        for match in EMAIL_RE.finditer(text):
            email = match.group().lower().rstrip(".")
            domain = email.split("@")[1]
            # Skip junk
            if domain in JUNK_EMAILS:
                continue
            # Skip image files that look like emails
            if email.endswith((".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js")):
                continue
            # Skip minified CSS/JS artifacts
            if len(email) > 60:
                continue
            found.add(email)
        return found

    def _extract_socials(self, html: str) -> Dict[str, str]:
        """Find social media URLs in HTML content."""
        found = {}
        for platform, pattern in SOCIAL_PATTERNS.items():
            matches = pattern.findall(html)
            if matches:
                # Clean up — take the shortest (most likely the profile, not a share link)
                url = min(matches, key=len)
                # Remove trailing slashes and fragments
                url = url.rstrip("/")
                found[platform] = url
        return found

    def _categorize_emails(self, emails: Set[str]) -> tuple:
        """Split emails into personal (name-based) and business (generic).
        Returns (personal_email, business_email) or (None, None).
        """
        personal = None
        business = None

        for email in emails:
            prefix = email.split("@")[0].lower()
            if prefix in GENERIC_PREFIXES:
                if not business:
                    business = email
            else:
                if not personal:
                    personal = email

        return personal, business
