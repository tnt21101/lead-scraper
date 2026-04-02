from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class BusinessLead(BaseModel):
    """Normalized lead record — every provider maps into this model."""

    # Core business info (from Google Maps scraping)
    business_name: str
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    google_maps_url: Optional[str] = None

    # Owner info (from enrichment)
    owner_name: Optional[str] = None
    owner_title: Optional[str] = None

    # Emails
    business_email: Optional[str] = None
    personal_email: Optional[str] = None
    email_confidence: Optional[float] = None

    # Social media — company
    company_linkedin: Optional[str] = None
    company_facebook: Optional[str] = None
    company_instagram: Optional[str] = None
    company_twitter: Optional[str] = None
    company_youtube: Optional[str] = None

    # Social media — owner
    owner_linkedin: Optional[str] = None
    owner_facebook: Optional[str] = None
    owner_twitter: Optional[str] = None

    # Metadata
    source_provider: str = ""
    enriched_by: List[str] = Field(default_factory=list)
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

    def merge(self, other: "BusinessLead") -> "BusinessLead":
        """Merge another lead's non-None fields into this one (enrichment)."""
        data = self.model_dump()
        for key, value in other.model_dump().items():
            if value is not None and data.get(key) is None:
                data[key] = value
        combined = list(set(self.enriched_by + other.enriched_by))
        data["enriched_by"] = combined
        return BusinessLead(**data)
