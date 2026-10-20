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


def test_workflow_is_dispatch_only():
    assert "workflow_dispatch" in _triggers(_load())


def test_workflow_carries_no_schedule_of_its_own():
    # GitHub runs scheduled workflows best-effort and drops them under
    # load; a walk needing a dozen invocations cannot be driven by a
    # trigger that silently skips most of them. An external scheduler
    # dispatches instead, and a `schedule:` here would double-trigger.
    assert "schedule" not in _triggers(_load())


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
