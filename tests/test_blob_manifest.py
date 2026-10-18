"""Tests for historical-ingestion progress state (Spec 010)."""
from datetime import date

from src.blob.manifest import (
    DONE_STATUSES,
    GIVE_UP_AFTER_ATTEMPTS,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
    Manifest,
    combo_id,
    month_id,
)
from src.blob.store import BlobStore
from tests.test_blob_store import FakeS3

MARCH = date(2024, 3, 1)
IE_LOAD = combo_id("entsoe", "IE", "load")
IE_GEN = combo_id("entsoe", "IE", "generation")


def _store(objects=None):
    client = FakeS3(objects)
    return BlobStore(
        token="tok",
        credentials_fn=lambda _t: {
            "endpoint": "https://example.invalid", "bucket": "b",
            "accessKeyId": "ak", "secretAccessKey": "sk",
            "sessionToken": "st", "expiresAt": 10**12,
        },
        client_factory=lambda _c: client,
    )


def test_month_id_sorts_chronologically_as_a_string():
    assert month_id(date(2024, 3, 1)) < month_id(date(2024, 10, 1))


def test_absent_manifest_loads_as_empty_rather_than_failing():
    assert Manifest.load(_store()).as_dict() == {}


def test_manifest_round_trips_through_the_store():
    store = _store()
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)
    manifest.save(store)

    assert Manifest.load(store).status(MARCH, IE_LOAD) == STATUS_SUCCESS


def test_success_and_unavailable_and_given_up_all_count_as_done():
    # `unavailable` is a positive acknowledgement from the source, and
    # `given_up` is the release valve — both let the walk move past.
    assert DONE_STATUSES == {STATUS_SUCCESS, STATUS_UNAVAILABLE, STATUS_GIVEN_UP}


def test_failed_does_not_count_as_done():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_FAILED)

    assert not manifest.is_done(MARCH, IE_LOAD)


def test_attempts_accumulate_across_failures():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_FAILED)
    manifest.record(MARCH, IE_LOAD, STATUS_FAILED)

    assert manifest.attempts(MARCH, IE_LOAD) == 2


def test_repeated_failure_escalates_to_given_up():
    manifest = Manifest()
    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        status = manifest.record(MARCH, IE_LOAD, STATUS_FAILED)

    assert status == STATUS_GIVEN_UP
    assert manifest.is_done(MARCH, IE_LOAD)


def test_attempts_freeze_once_a_combination_is_done():
    # The count exists to drive escalation. Bumping it for a
    # combination nothing was attempted for would make it climb for as
    # long as a sibling keeps the month open.
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)
    for _ in range(5):
        manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)

    assert manifest.attempts(MARCH, IE_LOAD) == 1


def test_a_done_combination_is_not_re_escalated_by_a_later_failure_report():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_UNAVAILABLE)

    assert manifest.record(MARCH, IE_LOAD, STATUS_FAILED) == STATUS_FAILED
    assert manifest.attempts(MARCH, IE_LOAD) == 1


def test_month_is_done_only_when_every_combination_is_done():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)
    manifest.record(MARCH, IE_GEN, STATUS_FAILED)

    assert not manifest.month_is_done(MARCH, [IE_LOAD, IE_GEN])

    manifest.record(MARCH, IE_GEN, STATUS_SUCCESS)
    assert manifest.month_is_done(MARCH, [IE_LOAD, IE_GEN])


def test_pending_combos_narrows_a_re_attempt_to_what_is_unresolved():
    # What makes a re-run cheap: an all-but-one month re-fetches one
    # combination, not the whole month.
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)

    assert manifest.pending_combos(MARCH, [IE_LOAD, IE_GEN]) == [IE_GEN]


def test_errors_are_recorded_and_truncated():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_FAILED, error="x" * 900)

    stored = manifest.as_dict()[month_id(MARCH)][IE_LOAD]["last_error"]
    assert len(stored) == 500


def test_reset_returns_given_up_combinations_to_the_walk():
    manifest = Manifest()
    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        manifest.record(MARCH, IE_GEN, STATUS_FAILED)
    assert manifest.is_done(MARCH, IE_GEN)

    assert manifest.reset(MARCH, [IE_GEN]) == 1
    assert not manifest.is_done(MARCH, IE_GEN)


def test_reset_clears_attempts_so_one_failure_does_not_re_escalate():
    manifest = Manifest()
    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        manifest.record(MARCH, IE_GEN, STATUS_FAILED)
    manifest.reset(MARCH, [IE_GEN])

    assert manifest.record(MARCH, IE_GEN, STATUS_FAILED) == STATUS_FAILED


def test_given_up_lists_what_needs_re_checking_oldest_first():
    manifest = Manifest()
    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        manifest.record(date(2024, 5, 1), IE_GEN, STATUS_FAILED)
    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        manifest.record(date(2024, 1, 1), IE_LOAD, STATUS_FAILED)

    assert manifest.given_up() == [("2024-01", IE_LOAD), ("2024-05", IE_GEN)]


def test_summary_counts_combinations_by_status():
    manifest = Manifest()
    manifest.record(MARCH, IE_LOAD, STATUS_SUCCESS)
    manifest.record(MARCH, IE_GEN, STATUS_UNAVAILABLE)

    assert manifest.summary() == {STATUS_SUCCESS: 1, STATUS_UNAVAILABLE: 1}
