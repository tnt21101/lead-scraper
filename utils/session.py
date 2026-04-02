"""Session state helpers for API key management."""

from typing import Dict, List, Optional

import streamlit as st


# Registry of supported providers and their key names
API_KEY_CONFIG: Dict[str, Dict] = {
    "outscraper": {
        "label": "Outscraper",
        "key_name": "outscraper_api_key",
        "type": "maps",
        "help": "Get your key at https://outscraper.com/",
        "free_tier": "500 businesses/month",
    },
    "serpapi": {
        "label": "SerpAPI",
        "key_name": "serpapi_api_key",
        "type": "maps",
        "help": "Get your key at https://serpapi.com/",
        "free_tier": "250 searches/month",
    },
    "scaleserp": {
        "label": "ScaleSERP",
        "key_name": "scaleserp_api_key",
        "type": "maps",
        "help": "Get your key at https://scaleserp.com/",
        "free_tier": "250 searches/month",
    },
    "hunter": {
        "label": "Hunter.io",
        "key_name": "hunter_api_key",
        "type": "email",
        "help": "Get your key at https://hunter.io/",
        "free_tier": "25 searches/month",
    },
    "apollo": {
        "label": "Apollo.io",
        "key_name": "apollo_api_key",
        "type": "social",
        "help": "Get your key at https://apollo.io/",
        "free_tier": "50 credits/month",
    },
}


def get_api_key(provider: str) -> Optional[str]:
    """Get an API key from session state."""
    config = API_KEY_CONFIG.get(provider)
    if not config:
        return None
    return st.session_state.get(config["key_name"]) or None


def set_api_key(provider: str, key: str) -> None:
    """Store an API key in session state."""
    config = API_KEY_CONFIG.get(provider)
    if config:
        st.session_state[config["key_name"]] = key


def get_configured_providers(provider_type: Optional[str] = None) -> List[str]:
    """Return provider IDs that have keys configured."""
    result = []
    for pid, config in API_KEY_CONFIG.items():
        if provider_type and config["type"] != provider_type:
            continue
        if get_api_key(pid):
            result.append(pid)
    return result


def has_any_maps_provider() -> bool:
    return len(get_configured_providers("maps")) > 0


def has_any_email_provider() -> bool:
    return len(get_configured_providers("email")) > 0


def has_any_social_provider() -> bool:
    return len(get_configured_providers("social")) > 0
