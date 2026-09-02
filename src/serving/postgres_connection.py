"""PostgreSQL connection handling (Spec 005).

Provider-agnostic: this connects to any standard PostgreSQL instance
(managed or self-hosted) via a connection URL or plain connection
parameters — nothing here is specific to a particular hosting provider.

Credentials are retrieved at runtime only, from the environment, and
never hard-coded. On Databricks these environment variables must be
populated from Databricks-managed secrets before this module is used
(e.g. in a notebook `os.environ[...] = dbutils.secrets.get(...)`) —
never logged, never committed.

Most managed PostgreSQL providers hand out a single connection URL
(`postgres://user:password@host:port/dbname?sslmode=require`) rather
than separate host/user/password fields, so that is the primary,
preferred input here (`POSTGRES_URL`). Discrete `POSTGRES_HOST` /
`POSTGRES_PORT` / etc. env vars remain supported as a fallback for
providers or setups that expose credentials that way instead.

No code in this repository calls `connect()` against a real database.
No PostgreSQL credentials are available in this environment, so an
actual connection has not been attempted or verified anywhere in this
codebase (Spec 005's live-database integration is a documented blocker —
see the project status notes).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlsplit

POSTGRES_URL_ENV_VAR = "POSTGRES_URL"

PG_HOST_ENV_VAR = "POSTGRES_HOST"
PG_PORT_ENV_VAR = "POSTGRES_PORT"
PG_DB_ENV_VAR = "POSTGRES_DB"
PG_USER_ENV_VAR = "POSTGRES_USER"
PG_PASSWORD_ENV_VAR = "POSTGRES_PASSWORD"
PG_SSLMODE_ENV_VAR = "POSTGRES_SSLMODE"

DEFAULT_PORT = 5432
DEFAULT_SSLMODE = "require"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10

_URL_SCHEMES = ("postgres", "postgresql")
_REQUIRED_ENV_VARS = (PG_HOST_ENV_VAR, PG_DB_ENV_VAR, PG_USER_ENV_VAR, PG_PASSWORD_ENV_VAR)


class MissingCredentialsError(RuntimeError):
    """Raised when required PostgreSQL connection details are not available."""


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
    """Build connection config from the environment, failing clearly if incomplete.

    Prefers a single `POSTGRES_URL` connection string (the shape most
    managed PostgreSQL providers actually hand out); falls back to
    discrete `POSTGRES_HOST`/`POSTGRES_PORT`/etc. env vars if no URL is
    set.
    """
    url = os.environ.get(POSTGRES_URL_ENV_VAR)
    if url:
        return _config_from_url(url)

    missing = [var for var in _REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise MissingCredentialsError(
            f"No PostgreSQL connection details found. Set {POSTGRES_URL_ENV_VAR} to a full "
            f"connection URL, or set all of: {list(_REQUIRED_ENV_VARS)}. On Databricks, "
            "populate these from Databricks-managed secrets before running."
        )

    return PostgresConnectionConfig(
        host=os.environ[PG_HOST_ENV_VAR],
        port=int(os.environ.get(PG_PORT_ENV_VAR, DEFAULT_PORT)),
        dbname=os.environ[PG_DB_ENV_VAR],
        user=os.environ[PG_USER_ENV_VAR],
        password=os.environ[PG_PASSWORD_ENV_VAR],
        sslmode=os.environ.get(PG_SSLMODE_ENV_VAR, DEFAULT_SSLMODE),
    )


def connect(config: PostgresConnectionConfig):
    """Open a real psycopg connection to PostgreSQL.

    This function is never called in this codebase's test suite — every
    test injects a mock connection object into the publishing functions
    instead, so no test claims a working connection to a database that
    was never actually reachable here.
    """
    import psycopg

    return psycopg.connect(**config.as_connect_kwargs())
