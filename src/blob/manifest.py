"""Historical-ingestion progress state (Spec 010).

A single JSON object in blob storage records, per
(month, source, country, dataset), whether that combination has been
landed and how many attempts it has taken. Progress is tracked
explicitly here and never inferred from what happens to be present in
the Parquet files or in Bronze: neither can distinguish "not attempted
yet" from "attempted, and the source legitimately has nothing".

Only the statuses in `DONE_STATUSES` let the walker move past a month.
`unavailable` counts as done because it is a positive acknowledgement
from the source, not merely an absence. `given_up` counts as done once a
combination has failed `GIVE_UP_AFTER_ATTEMPTS` times, so one
persistently failing country cannot hold up every older month — it stays
a distinct status from `unavailable` precisely so it can be found and
re-attempted later by resetting it to `failed`.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone as dt_timezone
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

MANIFEST_KEY_SUFFIX = "_manifest.json"

STATUS_SUCCESS = "success"
STATUS_UNAVAILABLE = "unavailable"
STATUS_FAILED = "failed"
STATUS_GIVEN_UP = "given_up"

# Small by intent: this is a release valve against one stuck combination
# blocking the walk, not a tolerance for ordinary transient failure. The
# scheduled re-runs are the real retry mechanism.
GIVE_UP_AFTER_ATTEMPTS = 3

DONE_STATUSES = frozenset({STATUS_SUCCESS, STATUS_UNAVAILABLE, STATUS_GIVEN_UP})


def combo_id(source: str, country_code: str, dataset: str) -> str:
    """Stable identifier for one country/dataset combination of one source."""
    return f"{source}:{country_code}:{dataset}"


def month_id(month_start: date) -> str:
    """Stable identifier for one month, sortable as a string."""
    return month_start.strftime("%Y-%m")


class Manifest:
    """In-memory view of the progress state, loaded from and saved to blob."""

    def __init__(self, data: Optional[Dict[str, dict]] = None):
        self._data: Dict[str, dict] = data or {}

    @classmethod
    def load(cls, store) -> "Manifest":
        """Read the manifest, treating an absent object as an empty one."""
        raw = store.get(store.key(MANIFEST_KEY_SUFFIX))
        if raw is None:
            logger.info("No manifest found; starting from empty state")
            return cls({})
        return cls(json.loads(raw.decode("utf-8")))

    def save(self, store) -> int:
        """Write the manifest back, sorted so successive versions diff cleanly."""
        payload = json.dumps(self._data, indent=2, sort_keys=True).encode("utf-8")
        return store.put(
            store.key(MANIFEST_KEY_SUFFIX), payload, content_type="application/json",
        )

    def status(self, month_start: date, combo: str) -> Optional[str]:
        return self._data.get(month_id(month_start), {}).get(combo, {}).get("status")

    def attempts(self, month_start: date, combo: str) -> int:
        return self._data.get(month_id(month_start), {}).get(combo, {}).get("attempts", 0)

    def is_done(self, month_start: date, combo: str) -> bool:
        return self.status(month_start, combo) in DONE_STATUSES

    def month_is_done(self, month_start: date, combos: Iterable[str]) -> bool:
        """True when every combination of a month has reached a done status."""
        return all(self.is_done(month_start, combo) for combo in combos)

    def pending_combos(self, month_start: date, combos: Iterable[str]) -> List[str]:
        """The combinations of a month still worth attempting.

        What makes a re-run cheap: a month sitting at all-but-one
        resolved re-fetches the one, not the whole month.
        """
        return [combo for combo in combos if not self.is_done(month_start, combo)]

    def record(
        self, month_start: date, combo: str, status: str, *, error: Optional[str] = None
    ) -> str:
        """Record the outcome of one attempt and return the status stored.

        A combination already in a done status keeps its attempt count:
        the count exists to drive escalation to `given_up`, and bumping
        it for a combination nothing was attempted for would make it
        climb for as long as some sibling keeps the month open.

        A `failed` outcome that reaches `GIVE_UP_AFTER_ATTEMPTS` is
        stored as `given_up` instead, which is what lets the walker move
        on.
        """
        key = month_id(month_start)
        month = self._data.setdefault(key, {})
        previous = month.get(combo, {})

        if previous.get("status") in DONE_STATUSES:
            attempts = previous.get("attempts", 0)
        else:
            attempts = previous.get("attempts", 0) + 1
            if status == STATUS_FAILED and attempts >= GIVE_UP_AFTER_ATTEMPTS:
                status = STATUS_GIVEN_UP

        entry = {
            "status": status,
            "attempts": attempts,
            "updated_at": datetime.now(dt_timezone.utc).isoformat(),
        }
        if error:
            entry["last_error"] = error[:500]
        month[combo] = entry
        return status

    def reset(self, month_start: date, combos: Iterable[str]) -> int:
        """Return combinations to `failed` so the walker re-attempts them.

        How a `given_up` combination is re-checked once the underlying
        cause is believed resolved. Attempt counts are cleared too,
        otherwise the first new failure would escalate straight back to
        `given_up`.
        """
        month = self._data.get(month_id(month_start), {})
        reset_count = 0
        for combo in combos:
            if combo in month:
                month[combo] = {
                    "status": STATUS_FAILED,
                    "attempts": 0,
                    "updated_at": datetime.now(dt_timezone.utc).isoformat(),
                }
                reset_count += 1
        return reset_count

    def summary(self) -> Dict[str, int]:
        """Count of combinations by status across every month recorded."""
        counts: Dict[str, int] = {}
        for month in self._data.values():
            for entry in month.values():
                status = entry.get("status", "unknown")
                counts[status] = counts.get(status, 0) + 1
        return counts

    def given_up(self) -> List[Tuple[str, str]]:
        """Every `(month_id, combo)` currently given up, oldest month first."""
        found = [
            (month_key, combo)
            for month_key, month in self._data.items()
            for combo, entry in month.items()
            if entry.get("status") == STATUS_GIVEN_UP
        ]
        return sorted(found)

    def as_dict(self) -> Dict[str, dict]:
        return self._data
