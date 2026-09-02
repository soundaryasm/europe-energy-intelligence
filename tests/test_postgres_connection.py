"""Tests for PostgreSQL connection config handling (Spec 005).

No real connection is ever attempted here — `connect()` itself is not
called by any test; only environment-variable resolution into a config
object is exercised.
"""
import pytest

from src.serving.postgres_connection import (
    DEFAULT_PORT,
    DEFAULT_SSLMODE,
    PG_DB_ENV_VAR,
    PG_HOST_ENV_VAR,
    PG_PASSWORD_ENV_VAR,
    PG_PORT_ENV_VAR,
    PG_USER_ENV_VAR,
    POSTGRES_URL_ENV_VAR,
    MissingCredentialsError,
    load_connection_config_from_env,
)


def _set_all_env(monkeypatch, **overrides):
    monkeypatch.delenv(POSTGRES_URL_ENV_VAR, raising=False)  # discrete-var path only
    values = {
        PG_HOST_ENV_VAR: "db.example.com",
        PG_DB_ENV_VAR: "energy",
        PG_USER_ENV_VAR: "app_user",
        PG_PASSWORD_ENV_VAR: "super-secret",
        **overrides,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_load_connection_config_from_env_reads_all_required_fields(monkeypatch):
    _set_all_env(monkeypatch)
    monkeypatch.setenv(PG_PORT_ENV_VAR, "12345")

    config = load_connection_config_from_env()

    assert config.host == "db.example.com"
    assert config.port == 12345
    assert config.dbname == "energy"
    assert config.user == "app_user"
    assert config.password == "super-secret"


def test_load_connection_config_from_env_defaults_port_and_sslmode(monkeypatch):
    _set_all_env(monkeypatch)
    monkeypatch.delenv(PG_PORT_ENV_VAR, raising=False)

    config = load_connection_config_from_env()

    assert config.port == DEFAULT_PORT
    assert config.sslmode == DEFAULT_SSLMODE


@pytest.mark.parametrize(
    "missing_var", [PG_HOST_ENV_VAR, PG_DB_ENV_VAR, PG_USER_ENV_VAR, PG_PASSWORD_ENV_VAR]
)
def test_load_connection_config_from_env_raises_when_a_required_var_is_missing(monkeypatch, missing_var):
    _set_all_env(monkeypatch)
    monkeypatch.delenv(missing_var, raising=False)

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_connection_config_repr_never_leaks_the_password(monkeypatch):
    _set_all_env(monkeypatch)
    config = load_connection_config_from_env()

    assert "super-secret" not in repr(config)


# --- POSTGRES_URL (the shape most managed providers actually hand out) ------
# NOTE: fabricated example credentials only — never a real connection string.

_EXAMPLE_URL = "postgres://app_user:s3cr3t-pass@db.example.com:15432/defaultdb?sslmode=require"


def test_load_connection_config_from_env_parses_a_full_url(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, _EXAMPLE_URL)

    config = load_connection_config_from_env()

    assert config.host == "db.example.com"
    assert config.port == 15432
    assert config.dbname == "defaultdb"
    assert config.user == "app_user"
    assert config.password == "s3cr3t-pass"
    assert config.sslmode == "require"


def test_load_connection_config_from_env_url_defaults_sslmode_and_port_when_absent(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://app_user:pw@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.port == DEFAULT_PORT
    assert config.sslmode == DEFAULT_SSLMODE


def test_load_connection_config_from_env_url_decodes_percent_encoded_password(monkeypatch):
    # A literal "@" in the password, percent-encoded as %40.
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://app_user:p%40ss@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.password == "p@ss"


def test_load_connection_config_from_env_url_accepts_postgresql_scheme_too(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgresql://app_user:pw@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.dbname == "energy"


def test_load_connection_config_from_env_url_rejects_wrong_scheme(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "mysql://app_user:pw@db.example.com/energy")

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_load_connection_config_from_env_url_rejects_incomplete_url(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://db.example.com/energy")  # no user

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_load_connection_config_from_env_prefers_url_over_discrete_vars(monkeypatch):
    _set_all_env(monkeypatch)  # sets discrete POSTGRES_HOST/etc.
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, _EXAMPLE_URL)

    config = load_connection_config_from_env()

    assert config.host == "db.example.com"
    assert config.dbname == "defaultdb"  # from the URL, not PG_DB_ENV_VAR's "energy"


def test_url_connection_config_repr_never_leaks_the_password(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, _EXAMPLE_URL)

    config = load_connection_config_from_env()

    assert "s3cr3t-pass" not in repr(config)
