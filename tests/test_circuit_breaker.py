"""Tests for the thread-safe, breadth-based circuit breaker."""
import threading

import pytest

from src.orchestration.circuit_breaker import CircuitBreaker


def test_starts_closed():
    breaker = CircuitBreaker(distinct_failure_threshold=3)
    assert breaker.is_open() is False


def test_stays_closed_below_threshold():
    breaker = CircuitBreaker(distinct_failure_threshold=3)
    breaker.record_failure("IE")
    breaker.record_failure("DE")
    assert breaker.is_open() is False


def test_opens_at_threshold():
    breaker = CircuitBreaker(distinct_failure_threshold=3)
    breaker.record_failure("IE")
    breaker.record_failure("DE")
    breaker.record_failure("FR")
    assert breaker.is_open() is True


def test_does_not_open_on_repeated_failures_of_the_same_identifier():
    # The exact real incident this guards against: one country's (or
    # indicator's) own datasets/windows all failing must not look like a
    # platform-wide outage — it's counted once, by identifier, not once
    # per failed call.
    breaker = CircuitBreaker(distinct_failure_threshold=3)
    for _ in range(10):
        breaker.record_failure("IE")
    assert breaker.is_open() is False
    assert breaker.failed_identifiers == {"IE"}


def test_rejects_non_positive_threshold():
    with pytest.raises(ValueError):
        CircuitBreaker(distinct_failure_threshold=0)


def test_thread_safe_under_concurrent_failures():
    # 20 threads each report a distinct failure concurrently — the
    # breaker must end up open (threshold 3) with no lost updates and no
    # crash, regardless of interleaving.
    breaker = CircuitBreaker(distinct_failure_threshold=3)
    identifiers = [f"C{i}" for i in range(20)]

    threads = [threading.Thread(target=breaker.record_failure, args=(ident,)) for ident in identifiers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert breaker.is_open() is True
    assert breaker.failed_identifiers == set(identifiers)
