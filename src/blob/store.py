"""S3-compatible blob access for the historical Parquet layer (Spec 010).

Thin wrapper over boto3's S3 client, holding the three things the rest of
this package should not have to know about: where the bucket lives, how
it is authenticated, and the key prefix every object in this project
shares.

Authentication is two-stage. The long-lived bucket token is exchanged at
`CREDENTIALS_URL` for temporary S3 credentials that carry their own
endpoint, bucket and session token. Those credentials expire after about
ten minutes, which is far shorter than a single historical ingestion run
— a year of history takes fifteen minutes on a good day and can stretch
past an hour against a degraded upstream. So the credentials are minted
on demand and re-minted whenever they are within `REFRESH_MARGIN_SECONDS`
of expiring, rather than once at construction. Every operation goes
through `_resolved()`, which is where that check happens.

Every key is namespaced under `KEY_PREFIX` so the bucket can host other
projects without collision, and so a listing scoped to this project is a
single prefixed call rather than a filter over everything in the bucket.

The bucket token comes from the environment, never from arguments or
config files, so the same code runs unchanged in CI and on Databricks:

    UPSTASH_BLOB_TOKEN     bucket token, exchanged for S3 credentials
"""
from __future__ import annotations

import logging
import os
import time
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

KEY_PREFIX = "europe-energy"

CREDENTIALS_URL = "https://blob.upstash.io/v1/credentials"

TOKEN_ENV_VAR = "UPSTASH_BLOB_TOKEN"

# Fallback when the credentials response carries no region; `auto` is
# what S3-compatible providers without a region concept expect.
DEFAULT_REGION = "auto"

# Re-mint this far ahead of expiry rather than at it, so an operation
# issued just under the wire does not race the expiry it just passed.
REFRESH_MARGIN_SECONDS = 60


class BlobConfigError(RuntimeError):
    """The bucket token is missing from the environment."""


class BlobCredentialError(RuntimeError):
    """The bucket token could not be exchanged for S3 credentials."""


def mint_credentials(token: str) -> Dict[str, object]:
    """Exchange the bucket token for temporary S3 credentials.

    Returns the provider's response, which carries `endpoint`, `bucket`,
    `accessKeyId`, `secretAccessKey`, `sessionToken` and `expiresAt`
    (unix seconds).
    """
    import requests

    response = requests.post(
        CREDENTIALS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=30,
    )
    if response.status_code != 200:
        raise BlobCredentialError(
            f"Credential exchange failed with HTTP {response.status_code}"
        )
    credentials = response.json()
    missing = [
        field
        for field in ("endpoint", "bucket", "accessKeyId", "secretAccessKey", "expiresAt")
        if field not in credentials
    ]
    if missing:
        raise BlobCredentialError(f"Credential response is missing fields: {missing}")
    return credentials


class BlobStore:
    """Read and write objects under this project's key prefix."""

    def __init__(
        self,
        *,
        token: str,
        credentials_fn: Callable[[str], Dict[str, object]] = mint_credentials,
        client_factory: Optional[Callable[[Dict[str, object]], object]] = None,
        time_fn: Callable[[], float] = time.time,
    ):
        self._token = token
        self._credentials_fn = credentials_fn
        self._client_factory = client_factory or self._build_client
        self._time_fn = time_fn
        self._credentials: Optional[Dict[str, object]] = None
        self._cached_client = None

    @classmethod
    def from_env(cls, **kwargs) -> "BlobStore":
        """Build a store from the bucket token in the environment."""
        token = os.environ.get(TOKEN_ENV_VAR)
        if not token:
            raise BlobConfigError(
                f"Missing required environment variable: {TOKEN_ENV_VAR}"
            )
        return cls(token=token, **kwargs)

    @staticmethod
    def _build_client(credentials: Dict[str, object]):
        # Imported here, not at module import time, so this module stays
        # importable (and unit-testable with an injected factory)
        # without boto3 installed.
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=credentials["endpoint"],
            region_name=credentials.get("region") or DEFAULT_REGION,
            aws_access_key_id=credentials["accessKeyId"],
            aws_secret_access_key=credentials["secretAccessKey"],
            aws_session_token=credentials.get("sessionToken"),
        )

    def _expired(self) -> bool:
        if self._credentials is None:
            return True
        expires_at = float(self._credentials["expiresAt"])
        return self._time_fn() >= expires_at - REFRESH_MARGIN_SECONDS

    def _resolved(self):
        """`(client, bucket)`, minting or re-minting credentials as needed.

        Returned together, and resolved exactly once per operation,
        because the bucket name comes from the same credential exchange
        as the client. Resolving them separately would run two
        independent expiry checks per call and could mint twice in a row
        at a refresh boundary.
        """
        if self._expired():
            self._credentials = self._credentials_fn(self._token)
            self._cached_client = self._client_factory(self._credentials)
            logger.info(
                "Minted blob credentials, valid for %ds",
                int(float(self._credentials["expiresAt"]) - self._time_fn()),
            )
        return self._cached_client, str(self._credentials["bucket"])

    @property
    def bucket(self) -> str:
        """Bucket name, which the credential exchange supplies."""
        return self._resolved()[1]

    @staticmethod
    def key(*parts: str) -> str:
        """Build a prefixed object key from path segments."""
        return "/".join((KEY_PREFIX,) + tuple(str(p).strip("/") for p in parts))

    def get(self, key: str) -> Optional[bytes]:
        """Object bytes, or `None` if the key does not exist.

        A missing key is an expected, meaningful state throughout this
        package (no manifest yet, a month not yet written), so it is
        reported as `None` rather than raised.
        """
        client, bucket = self._resolved()
        try:
            response = client.get_object(Bucket=bucket, Key=key)
        except Exception as exc:  # noqa: BLE001 - provider-specific not-found types
            if self._is_not_found(exc):
                return None
            raise
        return response["Body"].read()

    def put(self, key: str, data: bytes, *, content_type: str = "application/octet-stream") -> int:
        """Write `data` to `key`, replacing any existing object. Returns bytes written."""
        client, bucket = self._resolved()
        client.put_object(
            Bucket=bucket, Key=key, Body=data, ContentType=content_type,
        )
        logger.info("Wrote %s (%d bytes)", key, len(data))
        return len(data)

    def list_keys(self, prefix: str = "") -> List[str]:
        """Every key under this project's prefix, optionally narrowed further.

        Pages through the provider's truncated responses so a caller
        never sees a partial listing and mistakes it for the full set.
        Credentials are re-resolved once per page, not per call within a
        page: a listing of a decade of history can outlive one set of
        credentials.
        """
        full_prefix = self.key(prefix) if prefix else KEY_PREFIX
        keys: List[str] = []
        token: Optional[str] = None
        while True:
            client, bucket = self._resolved()
            kwargs = {"Bucket": bucket, "Prefix": full_prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = client.list_objects_v2(**kwargs)
            keys.extend(item["Key"] for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                return keys
            token = response.get("NextContinuationToken")

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        return code in ("NoSuchKey", "404", "NotFound") or exc.__class__.__name__ == "NoSuchKey"
