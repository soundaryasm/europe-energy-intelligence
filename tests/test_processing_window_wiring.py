"""Every windowed task is actually handed the window (Spec 006).

Spec 006 requires one canonical processing window passed to downstream
tasks rather than each task deriving its own. Silver and the serving
publish read that window from job parameters, so a task left without
them would silently fall back to the notebook widget defaults — a
3-day `daily` window — and quietly publish almost nothing during a
historical rebuild. Static YAML checks only; no workspace is contacted.
"""
from pathlib import Path

import pytest
import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"

WINDOW_PARAMS = ("execution_mode", "start_date", "end_date", "lookback_days")

# Task key -> the file and job it belongs to. Every notebook task that
# resolves a processing window must appear here.
WINDOWED_TASKS = (
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "ingest_entsoe"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "ingest_open_meteo"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "transform_silver"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "publish_postgres"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild", "transform_silver"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild", "publish_postgres"),
)


def _job(filename, job_key):
    with open(RESOURCES / filename, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["resources"]["jobs"][job_key]


def _task(filename, job_key, task_key):
    for task in _job(filename, job_key)["tasks"]:
        if task["task_key"] == task_key:
            return task
    raise AssertionError(f"{task_key} not found in {filename}")


@pytest.mark.parametrize("filename,job_key,task_key", WINDOWED_TASKS)
def test_a_windowed_task_receives_every_window_parameter(filename, job_key, task_key):
    params = _task(filename, job_key, task_key)["notebook_task"]["base_parameters"]
    for name in WINDOW_PARAMS:
        assert params.get(name) == "{{job.parameters.%s}}" % name, (
            f"{task_key} in {filename} does not pass {name} through from job parameters"
        )


@pytest.mark.parametrize("filename,job_key", [
    ("daily_pipeline.yml", "europe_energy_intelligence_daily"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild"),
])
def test_the_job_declares_every_window_parameter(filename, job_key):
    declared = {p["name"] for p in _job(filename, job_key)["parameters"]}
    assert set(WINDOW_PARAMS) <= declared


def test_the_daily_job_defaults_to_a_narrow_daily_window():
    # A scheduled run must never span the history (Spec 006).
    params = {p["name"]: p["default"] for p in
              _job("daily_pipeline.yml", "europe_energy_intelligence_daily")["parameters"]}
    assert params["execution_mode"] == "daily"
    assert params["start_date"] == ""
    assert params["end_date"] == ""
    assert int(params["lookback_days"]) <= 7


def test_the_rebuild_job_defaults_to_reprocess_from_the_start_of_history():
    params = {p["name"]: p["default"] for p in
              _job("historical_rebuild.yml",
                   "europe_energy_intelligence_historical_rebuild")["parameters"]}
    assert params["execution_mode"] == "reprocess"
    assert params["start_date"] == "2015-01-01"
    # Deliberately empty: reprocess requires an explicit end_date, so a
    # run that forgets to pass one fails loudly instead of silently
    # rebuilding a stale range.
    assert params["end_date"] == ""
