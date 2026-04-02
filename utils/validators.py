"""Input validation helpers."""

import re
from typing import Optional
from urllib.parse import urlparse


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def extract_domain(url: str) -> Optional[str]:
    """Extract the root domain from a URL."""
    try:
        parsed = urlparse(url if "://" in url else "https://%s" % url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = re.sub(r"^www\.", "", domain)
        return domain if domain else None
    except Exception:
        return None


def sanitize_query(query: str) -> str:
    """Clean up a search query string."""
    return " ".join(query.strip().split())
