"""Tests for blob-storage access (Spec 010).

The credential exchange and the S3 client are the external systems here
and are both replaced with doubles; these tests never touch the network.
"""
import pytest

from src.blob.store import (
    KEY_PREFIX,
    REFRESH_MARGIN_SECONDS,
    BlobConfigError,
    BlobCredentialError,
    BlobStore,
    mint_credentials,
)

BUCKET = "test-bucket"


class FakeS3:
    """Minimal in-memory stand-in for the boto3 S3 client surface used."""

    def __init__(self, objects=None, page_size=None, label="c0"):
        self.objects = dict(objects or {})
        self.page_size = page_size
        self.label = label
        self.put_calls = []

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3 argument names
        if Key not in self.objects:
            raise _NoSuchKey()
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.objects[Key] = Body
        self.put_calls.append((Key, ContentType))

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        if self.page_size is None:
            return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}
        start = int(ContinuationToken or 0)
        page = keys[start:start + self.page_size]
        nxt = start + self.page_size
        return {
            "Contents": [{"Key": k} for k in page],
            "IsTruncated": nxt < len(keys),
            "NextContinuationToken": str(nxt),
        }


class _NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class Clock:
    def __init__(self, now=1_000.0):
        self.now = now

    def __call__(self):
        return self.now


def _credentials(expires_at, label="c0"):
    return {
        "endpoint": "https://example.invalid",
        "bucket": BUCKET,
        "accessKeyId": "ak",
        "secretAccessKey": "sk",
        "sessionToken": "st",
        "expiresAt": expires_at,
        "label": label,
    }


def _store(objects=None, *, page_size=None, clock=None, lifetime=600):
    clock = clock or Clock()
    shared = FakeS3(objects, page_size=page_size)
    mints = []

    def credentials_fn(_token):
        mints.append(clock())
        return _credentials(clock() + lifetime, label=f"c{len(mints)}")

    store = BlobStore(
        token="tok",
        credentials_fn=credentials_fn,
        client_factory=lambda creds: shared,
        time_fn=clock,
    )
    return store, shared, mints


def test_keys_are_namespaced_under_the_project_prefix():
    assert BlobStore.key("entsoe", "year=2024", "month=03.parquet") == (
        f"{KEY_PREFIX}/entsoe/year=2024/month=03.parquet"
    )


def test_key_segments_do_not_double_up_separators():
    assert BlobStore.key("/entsoe/", "/x/") == f"{KEY_PREFIX}/entsoe/x"


def test_bucket_comes_from_the_credential_exchange():
    store, _, _ = _store()

    assert store.bucket == BUCKET


def test_credentials_are_minted_once_and_then_reused():
    store, _, mints = _store({f"{KEY_PREFIX}/a": b"x"})
    store.get(f"{KEY_PREFIX}/a")
    store.get(f"{KEY_PREFIX}/a")

    assert len(mints) == 1


def test_credentials_are_re_minted_once_they_near_expiry():
    # Credentials live about ten minutes; a single historical run is far
    # longer than that, so the store has to re-mint mid-run rather than
    # fail part-way through a year.
    clock = Clock()
    store, _, mints = _store({f"{KEY_PREFIX}/a": b"x"}, clock=clock, lifetime=600)
    store.get(f"{KEY_PREFIX}/a")

    clock.now += 600
    store.get(f"{KEY_PREFIX}/a")

    assert len(mints) == 2


def test_re_mint_happens_ahead_of_expiry_not_at_it():
    # An operation issued just under the wire must not race the expiry
    # it has already effectively passed.
    clock = Clock()
    store, _, mints = _store({f"{KEY_PREFIX}/a": b"x"}, clock=clock, lifetime=600)
    store.get(f"{KEY_PREFIX}/a")

    clock.now += 600 - REFRESH_MARGIN_SECONDS
    store.get(f"{KEY_PREFIX}/a")

    assert len(mints) == 2


def test_credentials_still_well_within_life_are_not_re_minted():
    clock = Clock()
    store, _, mints = _store({f"{KEY_PREFIX}/a": b"x"}, clock=clock, lifetime=600)
    store.get(f"{KEY_PREFIX}/a")

    clock.now += 60
    store.get(f"{KEY_PREFIX}/a")

    assert len(mints) == 1


def test_one_operation_resolves_credentials_only_once():
    # The bucket name comes from the same exchange as the client.
    # Resolving them separately ran two independent expiry checks per
    # call and could mint twice in a row at a refresh boundary.
    clock = Clock()
    store, _, mints = _store({f"{KEY_PREFIX}/a": b"x"}, clock=clock, lifetime=600)
    clock.now += 600

    store.get(f"{KEY_PREFIX}/a")

    assert len(mints) == 1


def test_a_listing_resolves_once_per_page_not_once_per_call():
    objects = {f"{KEY_PREFIX}/entsoe/{i}.parquet": b"" for i in range(6)}
    clock = Clock()
    store, _, mints = _store(objects, page_size=2, clock=clock, lifetime=600)

    store.list_keys()

    # Three pages, one credential window: a single mint covers them.
    assert len(mints) == 1


def test_region_from_the_credential_response_is_preferred():
    captured = {}
    BlobStore(
        token="t",
        credentials_fn=lambda _t: {
            "endpoint": "https://e.invalid", "bucket": "b", "accessKeyId": "ak",
            "secretAccessKey": "sk", "expiresAt": 10**12, "region": "weur",
        },
        client_factory=lambda creds: captured.setdefault("region", creds.get("region"))
        or FakeS3(),
    ).bucket

    assert captured["region"] == "weur"


def test_get_returns_none_for_a_missing_key_rather_than_raising():
    # A missing key is a meaningful state throughout this package (no
    # manifest yet, a month not yet written), not an error.
    store, _, _ = _store()

    assert store.get("absent") is None


def test_get_returns_stored_bytes():
    store, _, _ = _store({"k": b"payload"})

    assert store.get("k") == b"payload"


def test_get_propagates_errors_that_are_not_missing_keys():
    class Boom(Exception):
        response = {"Error": {"Code": "AccessDenied"}}

    store, client, _ = _store()

    def denied(Bucket, Key):  # noqa: N803
        raise Boom()

    client.get_object = denied

    with pytest.raises(Boom):
        store.get("k")


def test_put_records_content_type_and_returns_byte_count():
    store, client, _ = _store()
    written = store.put("k", b"12345", content_type="application/json")

    assert written == 5
    assert client.put_calls == [("k", "application/json")]


def test_list_keys_pages_through_truncated_responses():
    # A caller must never receive a partial listing and mistake it for
    # the full set.
    objects = {f"{KEY_PREFIX}/entsoe/{i}.parquet": b"" for i in range(7)}
    store, _, _ = _store(objects, page_size=2)

    assert len(store.list_keys()) == 7


def test_list_keys_can_be_narrowed_to_a_sub_prefix():
    objects = {
        f"{KEY_PREFIX}/entsoe/a.parquet": b"",
        f"{KEY_PREFIX}/open_meteo/b.parquet": b"",
    }
    store, _, _ = _store(objects)

    assert store.list_keys("entsoe") == [f"{KEY_PREFIX}/entsoe/a.parquet"]


def test_from_env_requires_the_bucket_token(monkeypatch):
    monkeypatch.delenv("UPSTASH_BLOB_TOKEN", raising=False)

    with pytest.raises(BlobConfigError) as exc:
        BlobStore.from_env()

    assert "UPSTASH_BLOB_TOKEN" in str(exc.value)


def test_from_env_builds_a_store_from_the_token(monkeypatch):
    monkeypatch.setenv("UPSTASH_BLOB_TOKEN", "tok")
    client = FakeS3({"x": b"y"})

    store = BlobStore.from_env(
        credentials_fn=lambda _t: _credentials(10**12),
        client_factory=lambda _c: client,
    )

    assert store.get("x") == b"y"


def test_credential_exchange_rejects_a_non_200_response(monkeypatch):
    class Response:
        status_code = 403

        def json(self):
            return {}

    monkeypatch.setitem(
        __import__("sys").modules, "requests",
        type("M", (), {"post": staticmethod(lambda *a, **k: Response())}),
    )

    with pytest.raises(BlobCredentialError):
        mint_credentials("tok")


def test_credential_exchange_rejects_an_incomplete_response(monkeypatch):
    class Response:
        status_code = 200

        def json(self):
            return {"endpoint": "https://example.invalid"}

    monkeypatch.setitem(
        __import__("sys").modules, "requests",
        type("M", (), {"post": staticmethod(lambda *a, **k: Response())}),
    )

    with pytest.raises(BlobCredentialError) as exc:
        mint_credentials("tok")

    assert "bucket" in str(exc.value)
