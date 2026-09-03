"""PostgreSQL connection handling (Spec 005).

Provider-agnostic: this connects to any standard PostgreSQL instance
(managed or self-hosted) via a single connection URL — nothing here is
specific to a particular hosting provider.

The connection URL is retrieved at runtime only, from the `POSTGRES_URL`
environment variable, and never hard-coded. On Databricks it must be
populated from a Databricks-managed secret before this module is used
(e.g. in a notebook `os.environ["POSTGRES_URL"] = dbutils.secrets.get(...)`)
— never logged, never committed.

No automated test in this repository calls `connect()` against a real
database — that stays intentionally true (Spec 005's live-database
integration must not be faked in CI). Basic connectivity via
`POSTGRES_URL` (including its `sslmode` query parameter) has been
manually confirmed to work with `psycopg.connect()` outside this
codebase's test suite, which validates the assumptions
`_config_from_url()`/`as_connect_kwargs()` are built on. The DDL in
`postgres_schema.py` and the upsert logic in `postgres_publisher.py`
have not themselves been exercised against a real database yet — that
remains an open integration step.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlsplit

POSTGRES_URL_ENV_VAR = "POSTGRES_URL"

DEFAULT_PORT = 5432
DEFAULT_SSLMODE = "require"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10

_URL_SCHEMES = ("postgres", "postgresql")


class MissingCredentialsError(RuntimeError):
    """Raised when `POSTGRES_URL` is missing or cannot be parsed."""


@dataclass(frozen=True)
class PostgresConnectionConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = DEFAULT_SSLMODE
    connect_timeout: int = DEFAULT_CONNECT_TIMEOUT_SECONDS

    def as_connect_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout,
        }

    def __repr__(self) -> str:  # never let a stray print() leak the password
        return f"PostgresConnectionConfig(host={self.host!r}, dbname={self.dbname!r}, user={self.user!r})"


def _config_from_url(url: str) -> PostgresConnectionConfig:
    parts = urlsplit(url)
    if parts.scheme not in _URL_SCHEMES:
        raise MissingCredentialsError(
            f"{POSTGRES_URL_ENV_VAR} must use the postgres:// or postgresql:// scheme, "
            f"got scheme: {parts.scheme!r}"
        )

    dbname = parts.path.lstrip("/")
    user = unquote(parts.username) if parts.username else ""
    password = unquote(parts.password) if parts.password else ""

    if not (parts.hostname and dbname and user):
        raise MissingCredentialsError(
            f"{POSTGRES_URL_ENV_VAR} is missing a host, database name, and/or user."
        )

    sslmode = parse_qs(parts.query).get("sslmode", [DEFAULT_SSLMODE])[0]

    return PostgresConnectionConfig(
        host=parts.hostname,
        port=parts.port or DEFAULT_PORT,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )


def load_connection_config_from_env() -> PostgresConnectionConfig:
    """Build connection config from `POSTGRES_URL`, failing clearly if unset."""
    url = os.environ.get(POSTGRES_URL_ENV_VAR)
    if not url:
        raise MissingCredentialsError(
            f"{POSTGRES_URL_ENV_VAR} is not set. On Databricks, populate it from a "
            "Databricks-managed secret before running."
        )
    return _config_from_url(url)


def connect(config: PostgresConnectionConfig):
    """Open a real psycopg connection to PostgreSQL.

    This function is never called in this codebase's test suite — every
    test injects a mock connection object into the publishing functions
    instead, so no test claims a working connection to a database that
    was never actually reachable here.
    """
    import psycopg

    return psycopg.connect(**config.as_connect_kwargs())
