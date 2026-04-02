from abc import ABC, abstractmethod
from typing import Callable, List, Optional

from core.models import BusinessLead


ProgressCallback = Callable[[int, int, str], None]  # (current, total, message)


class MapsScraper(ABC):
    """Interface for Google Maps scraping providers."""

    @abstractmethod
    def scrape(
        self,
        query: str,
        limit: int = 20,
        on_progress: Optional[ProgressCallback] = None,
    ) -> List[BusinessLead]:
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...


class EmailEnricher(ABC):
    """Interface for email enrichment providers."""

    @abstractmethod
    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...


class SocialEnricher(ABC):
    """Interface for social media enrichment providers."""

    @abstractmethod
    def enrich(
        self,
        lead: BusinessLead,
        on_progress: Optional[ProgressCallback] = None,
    ) -> BusinessLead:
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...
