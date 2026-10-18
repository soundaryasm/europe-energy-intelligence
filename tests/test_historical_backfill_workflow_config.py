"""Static validation of the historical ingestion workflow config (Spec 010).

Parses the GitHub Actions workflow and checks the properties Spec 010
requires: one invocation per fire, no overlap, a bounded timeout, and
credentials supplied as secrets rather than inline.
"""
from pathlib import Path

import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "historical-backfill.yml"
)


def _load():
    with open(WORKFLOW_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _triggers(workflow):
    # YAML parses a bare `on` key as the boolean True.
    return workflow.get("on", workflow.get(True))


def _job(workflow):
    return workflow["jobs"]["resolve-year"]


def test_workflow_runs_on_a_schedule():
    assert "schedule" in _triggers(_load())


def test_workflow_can_also_be_triggered_by_hand():
    assert "workflow_dispatch" in _triggers(_load())


def test_schedule_spacing_exceeds_the_worst_case_year():
    # Spec 010: spacing must exceed the worst-case duration of one year,
    # not its typical duration, so invocations cannot overlap.
    crons = [entry["cron"] for entry in _triggers(_load())["schedule"]]

    assert crons == ["0 1-23/2 * * *"]


def test_schedule_fires_on_odd_hours_and_never_at_midnight():
    # Nothing at 00:00, where scheduled workloads bunch up across the
    # platform and a fire is most likely to be delayed or dropped.
    cron = _triggers(_load())["schedule"][0]["cron"]
    minute, hour, *_ = cron.split()

    assert minute == "0"
    assert hour == "1-23/2"


def test_concurrent_invocations_are_prevented():
    # ENTSO-E rate-limits per token as well as per IP, and this workflow
    # holds the only token.
    concurrency = _load()["concurrency"]

    assert concurrency["group"]
    assert concurrency["cancel-in-progress"] is False


def test_the_job_has_a_bounded_timeout_below_the_platform_default():
    # A hung run should die rather than consume the monthly allowance up
    # to the 6-hour default.
    assert _job(_load())["timeout-minutes"] == 120


def test_the_job_runs_on_linux_which_bills_at_the_lowest_multiplier():
    assert _job(_load())["runs-on"] == "ubuntu-latest"


def test_credentials_are_supplied_as_secrets_not_literals():
    env = next(
        step["env"] for step in _job(_load())["steps"] if "env" in step
    )

    for value in env.values():
        assert value.startswith("${{ secrets.")


def test_both_tokens_the_run_needs_are_supplied():
    env = next(step["env"] for step in _job(_load())["steps"] if "env" in step)

    assert set(env) == {"ENTSOE_API_TOKEN", "UPSTASH_BLOB_TOKEN"}


def test_the_entrypoint_the_workflow_invokes_exists():
    workflow = _load()
    run_step = next(
        step for step in _job(workflow)["steps"] if step.get("run", "").startswith("python ")
    )
    script = run_step["run"].split()[-1]

    assert (Path(__file__).resolve().parents[1] / script).is_file()
