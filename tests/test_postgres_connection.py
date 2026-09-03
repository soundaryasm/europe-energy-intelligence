"""Tests for PostgreSQL connection config handling (Spec 005).

No real connection is ever attempted here — `connect()` itself is not
called by any test; only `POSTGRES_URL` parsing into a config object is
exercised.

NOTE: fabricated example credentials only — never a real connection
string.
"""
import pytest

from src.serving.postgres_connection import (
    DEFAULT_PORT,
    DEFAULT_SSLMODE,
    POSTGRES_URL_ENV_VAR,
    MissingCredentialsError,
    load_connection_config_from_env,
)

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


def test_load_connection_config_from_env_defaults_sslmode_and_port_when_absent(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://app_user:pw@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.port == DEFAULT_PORT
    assert config.sslmode == DEFAULT_SSLMODE


def test_load_connection_config_from_env_decodes_percent_encoded_password(monkeypatch):
    # A literal "@" in the password, percent-encoded as %40.
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://app_user:p%40ss@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.password == "p@ss"


def test_load_connection_config_from_env_accepts_postgresql_scheme_too(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgresql://app_user:pw@db.example.com/energy")

    config = load_connection_config_from_env()

    assert config.dbname == "energy"


def test_load_connection_config_from_env_rejects_wrong_scheme(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "mysql://app_user:pw@db.example.com/energy")

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_load_connection_config_from_env_rejects_incomplete_url(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, "postgres://db.example.com/energy")  # no user

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_load_connection_config_from_env_raises_when_url_is_not_set(monkeypatch):
    monkeypatch.delenv(POSTGRES_URL_ENV_VAR, raising=False)

    with pytest.raises(MissingCredentialsError):
        load_connection_config_from_env()


def test_connection_config_repr_never_leaks_the_password(monkeypatch):
    monkeypatch.setenv(POSTGRES_URL_ENV_VAR, _EXAMPLE_URL)

    config = load_connection_config_from_env()

    assert "s3cr3t-pass" not in repr(config)
