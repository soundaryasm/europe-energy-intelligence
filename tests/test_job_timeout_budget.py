"""Static validation of every job's timeout budget.

A hung task used to cost an hour and then start a second one: every
task-level `timeout_seconds` was 13-90x the task's real runtime, and
there was no job-level cap at all, so a bad night could stack 10.5 hours
of serverless compute against a Free Edition allowance shared with other
projects. These tests pin the ceiling so it cannot drift back.

Observed runtimes on 2026-11-05 (full successful run, 425s wall clock):
worldbank 39s, entsoe 274s, open_meteo 67s, silver 89s, gold 87s,
publish 23s.
"""
from pathlib import Path

import yaml

JOB_PATH = Path(__file__).resolve().parents[1] / "resources" / "daily_pipeline.yml"

# Longest each task has been observed to take, in seconds.
OBSERVED = {
    "ingest_worldbank": 39,
    "ingest_entsoe": 274,
    "ingest_open_meteo": 67,
    "transform_silver": 89,
    "build_dbt_gold": 87,
    "publish_postgres": 23,
}

# A normal run is ~7 minutes; the job cap must stay far below the old
# 10.5-hour worst case while still fitting one slow entsoe retry.
MAX_JOB_TIMEOUT_SECONDS = 2700


def _job():
    with open(JOB_PATH, "r", encoding="utf-8") as fh:
        bundle = yaml.safe_load(fh)
    return bundle["resources"]["jobs"]["europe_energy_intelligence_daily"]


def _tasks():
    return {t["task_key"]: t for t in _job()["tasks"]}


def test_the_job_has_a_hard_timeout():
    # The backstop the task-level timeouts cannot provide: they bound
    # one task each, so without this a run can stack all of them.
    job = _job()
    assert job.get("timeout_seconds"), "daily job must declare a job-level timeout_seconds"
    assert job["timeout_seconds"] <= MAX_JOB_TIMEOUT_SECONDS


def test_every_task_declares_a_timeout():
    for key, task in _tasks().items():
        assert task.get("timeout_seconds"), f"{key} has no timeout_seconds"


# A floor, because a ratio alone is wrong for the quick tasks: a 23s
# publish still has to wake Neon from scale-to-zero, and a 230s cap
# would fail a run for being briefly slow rather than for hanging.
MIN_TASK_TIMEOUT_SECONDS = 300


def test_no_task_timeout_is_wildly_above_its_real_runtime():
    # 10x observed is generous for a slow upstream or a cold start, and
    # still cheap to lose. Beyond that the timeout stops being a guard.
    for key, observed in OBSERVED.items():
        timeout = _tasks()[key]["timeout_seconds"]
        ceiling = max(observed * 10, MIN_TASK_TIMEOUT_SECONDS)
        assert timeout <= ceiling, (
            f"{key}: timeout {timeout}s exceeds {ceiling}s "
            f"(10x its observed {observed}s, floor {MIN_TASK_TIMEOUT_SECONDS}s)"
        )


def test_the_entsoe_retry_gap_still_outlasts_the_rate_limit_ban():
    # ENTSO-E bans a token for 10 minutes when its rate limit is
    # exceeded, so retrying sooner just fails for the same reason. The
    # gap is wall-clock waiting, not compute, so it costs nothing.
    gap_ms = _tasks()["ingest_entsoe"].get("min_retry_interval_millis", 0)
    assert gap_ms >= 600_000


def test_a_slow_entsoe_retry_still_fits_inside_the_job_cap():
    # The expensive legitimate path: entsoe times out, waits out its
    # retry gap, succeeds on the second attempt, then the rest of the
    # pipeline runs. This must not be killed by the job cap.
    tasks = _tasks()
    entsoe = tasks["ingest_entsoe"]
    worst_entsoe = (
        entsoe["timeout_seconds"]
        + entsoe.get("min_retry_interval_millis", 0) / 1000
        + OBSERVED["ingest_entsoe"]
    )
    downstream = sum(
        OBSERVED[k] for k in ("transform_silver", "build_dbt_gold", "publish_postgres")
    )
    assert worst_entsoe + downstream < _job()["timeout_seconds"]


# The two manual jobs. Their longest observed runs, in seconds, from the
# full historical sequence on 2026-11-04: the Silver/Gold rebuild over
# all of 2015-2026 took 318s, and the Bronze load merging 36.5M rows
# took 450s. Their task timeouts were originally 2-3 hours each, 24-40x
# reality, with no job-level cap — 8h and 7h of unbounded stack.
MANUAL_JOBS = {
    "resources/historical_rebuild.yml": ("europe_energy_intelligence_historical_rebuild", 318, 5400),
    "resources/historical_bronze_load.yml": ("europe_energy_intelligence_historical_bronze_load", 450, 7200),
}


def _bundle_job(rel_path, key):
    path = Path(__file__).resolve().parents[1] / rel_path
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["resources"]["jobs"][key]


def test_every_job_declares_a_job_level_timeout():
    # The daily job is the unattended one, but a manual job left hanging
    # burns the same shared allowance.
    for rel_path, (key, _, cap) in MANUAL_JOBS.items():
        job = _bundle_job(rel_path, key)
        assert job.get("timeout_seconds"), f"{key} has no job-level timeout_seconds"
        assert job["timeout_seconds"] <= cap


def test_manual_job_tasks_stay_within_reach_of_their_real_runtime():
    # Looser than the daily job's 10x: a full-history rebuild is rare,
    # and killing a legitimately slow one costs more than the compute.
    # 30x the whole job's observed duration is still far below the
    # 2-3 hour timeouts these started with.
    for rel_path, (key, observed, _) in MANUAL_JOBS.items():
        job = _bundle_job(rel_path, key)
        for task in job["tasks"]:
            timeout = task["timeout_seconds"]
            assert timeout <= observed * 30, (
                f"{key}/{task['task_key']}: timeout {timeout}s is more than "
                f"30x the job's observed {observed}s"
            )


def test_no_job_can_outlast_its_own_cap_by_stacking_tasks():
    # A job cap below the sum of its task timeouts is the point: it is
    # what stops six one-hour hangs becoming a six-hour run.
    everything = dict(MANUAL_JOBS)
    everything["resources/daily_pipeline.yml"] = (
        "europe_energy_intelligence_daily", None, MAX_JOB_TIMEOUT_SECONDS,
    )
    for rel_path, (key, _, _) in everything.items():
        job = _bundle_job(rel_path, key)
        stacked = sum(
            t["timeout_seconds"] * (1 + t.get("max_retries", 0))
            + t.get("min_retry_interval_millis", 0) / 1000 * t.get("max_retries", 0)
            for t in job["tasks"]
        )
        assert job["timeout_seconds"] < stacked, (
            f"{key}: job cap {job['timeout_seconds']}s is not below its "
            f"{stacked:.0f}s task stack, so it caps nothing"
        )
