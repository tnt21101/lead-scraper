"""Session state helpers for API key management."""

from typing import Dict, List, Optional

import streamlit as st


API_KEY_CONFIG: Dict[str, Dict] = {
    "outscraper": {
        "label": "Outscraper",
        "key_name": "outscraper_api_key",
        "type": "maps",
        "help": "https://outscraper.com/",
    },
    "serpapi": {
        "label": "SerpAPI",
        "key_name": "serpapi_api_key",
        "type": "maps",
        "help": "https://serpapi.com/",
    },
    "scaleserp": {
        "label": "ScaleSERP",
        "key_name": "scaleserp_api_key",
        "type": "maps",
        "help": "https://scaleserp.com/",
    },
    "hunter": {
        "label": "Hunter.io",
        "key_name": "hunter_api_key",
        "type": "email",
        "help": "https://hunter.io/",
    },
    "apollo": {
        "label": "Apollo.io",
        "key_name": "apollo_api_key",
        "type": "both",
        "help": "https://apollo.io/",
    },
}


def get_api_key(provider: str) -> Optional[str]:
    config = API_KEY_CONFIG.get(provider)
    if not config:
        return None
    return st.session_state.get(config["key_name"]) or None


def set_api_key(provider: str, key: str) -> None:
    config = API_KEY_CONFIG.get(provider)
    if config:
        st.session_state[config["key_name"]] = key


def get_configured_providers(provider_type: Optional[str] = None) -> List[str]:
    result = []
    for pid, config in API_KEY_CONFIG.items():
        ptype = config["type"]
        if provider_type:
            # "both" matches either "email" or "social"
            if ptype == "both" and provider_type in ("email", "social"):
                pass  # matches
            elif ptype != provider_type:
                continue
        if get_api_key(pid):
            result.append(pid)
    return result
