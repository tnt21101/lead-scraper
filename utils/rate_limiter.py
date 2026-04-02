"""Simple token-bucket rate limiter for API calls."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """Token bucket rate limiter.

    Args:
        calls_per_second: Maximum calls allowed per second.
    """

    calls_per_second: float = 1.0
    _last_call: float = field(default=0.0, init=False, repr=False)

    def wait(self) -> None:
        """Block until the next call is allowed."""
        if self._last_call == 0.0:
            self._last_call = time.monotonic()
            return

        min_interval = 1.0 / self.calls_per_second
        elapsed = time.monotonic() - self._last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_call = time.monotonic()
