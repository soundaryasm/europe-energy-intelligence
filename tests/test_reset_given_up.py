"""Tests for returning given-up combinations to the walk (Spec 010)."""
from datetime import date

import pytest

from scripts.reset_given_up import main
from src.blob.manifest import (
    GIVE_UP_AFTER_ATTEMPTS,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    Manifest,
    combo_id,
)
from src.blob.store import BlobStore
from tests.test_blob_store import FakeS3

DECEMBER = date(2025, 12, 1)
IE_GEN = combo_id("entsoe", "IE", "generation")
DE_WEATHER = combo_id("open_meteo", "DE", "weather")


@pytest.fixture
def store(monkeypatch):
    client = FakeS3()
    built = BlobStore(
        token="tok",
        credentials_fn=lambda _t: {
            "endpoint": "https://example.invalid", "bucket": "b",
            "accessKeyId": "ak", "secretAccessKey": "sk",
            "sessionToken": "st", "expiresAt": 10**12,
        },
        client_factory=lambda _c: client,
    )
    monkeypatch.setattr(
        "scripts.reset_given_up.BlobStore",
        type("S", (), {"from_env": staticmethod(lambda **k: built)}),
    )
    return built


def _given_up_manifest(store, combos, month=DECEMBER):
    manifest = Manifest()
    for combo in combos:
        for _ in range(GIVE_UP_AFTER_ATTEMPTS):
            manifest.record(month, combo, STATUS_FAILED)
        assert manifest.status(month, combo) == STATUS_GIVEN_UP
    manifest.save(store)
    return manifest


def test_reporting_changes_nothing(store):
    _given_up_manifest(store, [IE_GEN])

    assert main([]) == 0
    assert Manifest.load(store).status(DECEMBER, IE_GEN) == STATUS_GIVEN_UP


def test_nothing_given_up_is_not_an_error(store):
    Manifest().save(store)

    assert main([]) == 0


def test_all_resets_every_given_up_combination(store):
    _given_up_manifest(store, [IE_GEN, DE_WEATHER])

    assert main(["--all"]) == 0

    reloaded = Manifest.load(store)
    assert reloaded.status(DECEMBER, IE_GEN) == STATUS_FAILED
    assert reloaded.status(DECEMBER, DE_WEATHER) == STATUS_FAILED


def test_reset_clears_the_attempt_count_so_it_does_not_re_escalate(store):
    # Leaving the count in place would escalate straight back to
    # `given_up` on the first new failure, making the reset pointless.
    _given_up_manifest(store, [IE_GEN])
    main(["--all"])

    reloaded = Manifest.load(store)
    assert reloaded.attempts(DECEMBER, IE_GEN) == 0
    assert reloaded.record(DECEMBER, IE_GEN, STATUS_FAILED) == STATUS_FAILED


def test_source_filter_leaves_other_sources_alone(store):
    _given_up_manifest(store, [IE_GEN, DE_WEATHER])

    assert main(["--source", "open_meteo"]) == 0

    reloaded = Manifest.load(store)
    assert reloaded.status(DECEMBER, DE_WEATHER) == STATUS_FAILED
    assert reloaded.status(DECEMBER, IE_GEN) == STATUS_GIVEN_UP


def test_month_filter_leaves_other_months_alone(store):
    manifest = Manifest()
    for month in (DECEMBER, date(2024, 8, 1)):
        for _ in range(GIVE_UP_AFTER_ATTEMPTS):
            manifest.record(month, IE_GEN, STATUS_FAILED)
    manifest.save(store)

    assert main(["--month", "2025-12"]) == 0

    reloaded = Manifest.load(store)
    assert reloaded.status(DECEMBER, IE_GEN) == STATUS_FAILED
    assert reloaded.status(date(2024, 8, 1), IE_GEN) == STATUS_GIVEN_UP


def test_country_filter_leaves_other_countries_alone(store):
    ie_weather = combo_id("open_meteo", "IE", "weather")
    _given_up_manifest(store, [ie_weather, DE_WEATHER])

    assert main(["--country", "IE"]) == 0

    reloaded = Manifest.load(store)
    assert reloaded.status(DECEMBER, ie_weather) == STATUS_FAILED
    assert reloaded.status(DECEMBER, DE_WEATHER) == STATUS_GIVEN_UP


def test_a_filter_matching_nothing_is_reported_as_a_failure(store):
    _given_up_manifest(store, [IE_GEN])

    assert main(["--month", "1999-01"]) == 2
