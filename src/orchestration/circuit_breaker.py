"""Thread-safe circuit breaker for bounded-concurrency ingestion.

Guards against the exact failure mode observed twice in production this
week: a genuinely down upstream API (ENTSO-E's platform outage) causing
every one of many independent, already-bounded (3-attempt) retries to
burn its full ~96s timeout+backoff budget one after another, for no
new information after the first few.

Trips on *breadth*, not raw failure count: N distinct identifiers
(country codes, indicator codes — whatever the caller's unit of work is)
failing, not N total failed requests. This deliberately does NOT trip on
3 failures confined to one identifier (e.g. one country's 3 datasets all
failing) — that is a per-identifier problem, not evidence the whole
upstream API is down, and a real incident this week (Ireland's ENTSO-E
load/generation failing while the other 14 countries worked fine) is
exactly the case a raw-count trigger would have gotten wrong.
"""
from __future__ import annotations

import threading
from typing import Optional, Set


class CircuitBreaker:
    """Opens once `distinct_failure_threshold` distinct identifiers have
    each reported at least one failure. Thread-safe: `record_failure`
    and `is_open` may be called concurrently from multiple worker
    threads.

    One instance per `run_ingestion` call — never shared across runs.
    """

    def __init__(self, distinct_failure_threshold: int = 3):
        if distinct_failure_threshold <= 0:
            raise ValueError("distinct_failure_threshold must be a positive integer")
        self._threshold = distinct_failure_threshold
        self._lock = threading.Lock()
        self._failed_identifiers: Set[str] = set()
        self._open = False

    def record_failure(self, identifier: str) -> None:
        with self._lock:
            self._failed_identifiers.add(identifier)
            if len(self._failed_identifiers) >= self._threshold:
                self._open = True

    def is_open(self) -> bool:
        with self._lock:
            return self._open

    @property
    def failed_identifiers(self) -> Set[str]:
        with self._lock:
            return set(self._failed_identifiers)


class CircuitOpenError(RuntimeError):
    """Raised (caught internally by the worker loop) when a unit of work
    is skipped because the circuit breaker opened partway through.
    """

    def __init__(self, identifier: str, failed_identifiers: Optional[Set[str]] = None):
        self.identifier = identifier
        self.failed_identifiers = failed_identifiers or set()
        super().__init__(
            f"Circuit breaker open before {identifier!r} could be attempted "
            f"(distinct failures so far: {sorted(self.failed_identifiers)})"
        )
