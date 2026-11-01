# 010 — Historical Blob Backfill

## Goal

Define how the project's historical range is ingested: who fetches it, where it lands, how progress is tracked, and how it reaches Bronze.

Historical ingestion is a separate layer from the daily workflow (`specs/006-daily-orchestration.md`). It has its own orchestration, its own progress state, and its own schedule, and it shares the daily workflow's ingestion code rather than duplicating it.

## Scope

Covered:

- January 2015 through the last complete calendar month
- ENTSO-E load, generation and price for every configured country
- Open-Meteo weather for every configured country
- World Bank indicators for every covered year

Not covered:

- the current month-to-date, which is not a complete month
- the recent-date window, which is the daily workflow's responsibility

## Why This Layer Runs Off Databricks

Historical ingestion is HTTP requests, Parquet serialisation and an object-store write. None of it needs Spark: the parsing and Bronze row construction are plain Python, and the only Spark-dependent step in the daily path is the Delta write, which this layer replaces with a Parquet write.

Databricks compute is a bounded monthly allowance shared with the daily pipeline. Ingesting a decade of history is a large, one-off, Spark-free workload, and running it on that allowance competes directly with the daily freshness the platform exists to provide. Running it on general-purpose CI instead leaves the allowance for the work that genuinely needs Spark.

Databricks remains the system of record. This layer produces files; it never writes Bronze, Silver or Gold.

## Execution Model

One invocation resolves one year.

The year chosen is the most recent year that still has an unresolved month, walking backwards towards January 2015. Recent history lands first, because that is the history most likely to be wanted while the rest is still filling.

Within the year, each month is fetched, classified and written, and the progress state is saved after every month. An invocation killed by a timeout keeps every month it completed.

An invocation that finds nothing left to resolve exits immediately without fetching, so a schedule left running after completion costs one short no-op per fire rather than re-fetching.

### Scheduling

Invocations are repeated by an external scheduler calling the CI provider's dispatch API, not by a schedule inside the workflow.

The CI provider runs scheduled workflows on a best-effort basis and drops them under load, leaving no record of a dropped fire. Measured on 2026-10-18: of eight fires due across a day, two were delivered, 28 and 58 minutes late, and moving off the top of the hour changed nothing. A walk that needs a dozen or so invocations to finish cannot be driven by a trigger that silently skips most of them.

Spacing must exceed the worst-case duration of one year rather than its typical duration. A year is roughly 2,700 ENTSO-E requests, typically around fifteen minutes; every request carries a bounded retry with exponential backoff and a read timeout, so a degraded upstream can stretch a year past an hour.

Concurrent invocations are not permitted. ENTSO-E rate-limits per token as well as per IP, and this layer holds the only token; two invocations fetching at once would share that budget and risk the ban that follows exceeding it. Serialisation is enforced in the workflow itself rather than relying on the scheduler's spacing.

## Storage Layout

Every object is namespaced under a single project prefix, so the bucket can host unrelated projects without collision and a listing scoped to this project is one prefixed call.

```text
europe-energy/
├── _manifest.json
├── entsoe/year=YYYY/month=MM.parquet
├── open_meteo/year=YYYY/month=MM.parquet
└── worldbank/year=YYYY.parquet
```

One file per source-month. A re-run of a year overwrites the same files rather than accumulating new ones, which is what makes the layer idempotent on the write side without any merge logic in the orchestration.

Parquet is written with a schema derived from the same field definitions as the Bronze Delta schemas, so a file loads into Bronze without a cast or a schema-evolution step. The column set, ordering and nullability have one definition, not two.

### Partial Months

A month may resolve only some of its country/dataset combinations. The next invocation must be able to add the rest without duplicating what is there.

So a month's file is rewritten in full on each write: the existing file is read, rows sharing a Bronze merge key are replaced by their newer counterparts, and the union is written back. "The same row" therefore means the same thing here as it does in the Delta merge downstream.

An attempt that produces no rows must leave any existing file untouched. It must never erase rows an earlier attempt landed.

## Progress State

Progress is tracked explicitly, in a single JSON object in blob storage, per (month, source, country, dataset).

It must never be inferred from what is present in the Parquet files or in Bronze. Neither can distinguish "not attempted yet" from "attempted, and the source legitimately has nothing".

### Statuses

| status | meaning | counts as done |
|---|---|---|
| `success` | every calendar date in the month is covered | yes |
| `unavailable` | the source positively confirmed it holds nothing | yes |
| `failed` | incomplete; retried on the next invocation | no |
| `given_up` | failed three times without ever being confirmed absent | yes |

Only a done status lets the walk move past a month.

`unavailable` counts as done because it is a positive acknowledgement from the source, not merely an absence.

`given_up` counts as done so that one persistently failing combination cannot hold up every older month indefinitely. It stays distinct from `unavailable` precisely so it can be found and re-attempted later: resetting those entries to `failed` returns them to the walk, and resetting clears the attempt count so a single new failure does not escalate straight back.

An invocation that abandons a combination must say so loudly and must not report success. `given_up` means that month is permanently short of data until someone intervenes, which is the opposite of a clean run. Only combinations abandoned during that invocation are reported, not the running total — reporting the total would mark every later invocation as a failure for the rest of the walk, which is as useless as saying nothing.

Abandonment is the *only* outcome that fails an invocation. A combination left `failed` is retried on the next one, and against a source that times out under load that happens on most runs. Failing the invocation for it makes the routine outcome indistinguishable from the one that needs a person, which is how the one that needs a person gets ignored. Those combinations are still logged as a warning and still named in the run summary.

### Attempt Counting

The attempt count exists only to drive escalation to `given_up`.

A combination already in a done status keeps its count. Bumping it for a combination nothing was attempted for would make it climb for as long as some sibling combination keeps the month open, which carries no information about escalation.

### Narrowing Re-attempts

An invocation fetches only the countries that still have an unresolved combination for that month. A month sitting at all-but-one resolved re-fetches the one country, not every country.

Narrowing is by country rather than by country and dataset: failures concentrate per country, and the ingestion modules take a country list, so this keeps a re-attempt cheap without reaching inside them.

## Completeness Classification

A month is accounted for only when every calendar date in it is covered. A month with some days present and some missing is `failed` and re-attempted — never advanced past on the grounds that it looks populated.

Coverage is computed in process, from the records the attempt just produced. It is not computed by querying what has already been stored, which keeps the classification free of any storage or engine dependency and makes it reflect exactly what the attempt achieved.

The distinction that must be preserved is between a month the source confirmed has no data and a month that merely came back empty. "Nothing came back" and "there is nothing there" are different claims. Only the former, evidenced by a positive signal from the source, is `unavailable`.

ENTSO-E provides that signal as an acknowledgement document. Open-Meteo provides none, so an empty weather month is always `failed`: a whole month with no weather is not a legitimate condition the way a missing market series can be.

### Former Bidding Zones

A country's configured ENTSO-E domain is the one it uses now. Where a country has changed bidding zone, that domain holds nothing for the years before the change, and ENTSO-E answers such a request with a clean "no matching data" acknowledgement rather than an error — so the month resolves as `unavailable` and looks settled when it is simply being asked under the wrong code.

A country whose every dataset comes back empty for a month is therefore re-requested under the domain it previously used, where one is known. Data found that way is recorded as success, not left marked unavailable: the data exists, and recording a confirmed absence that is not true is worse than not asking.

Only a country where no dataset succeeded is retried. One dataset missing while the others returned data is a genuine gap in that series, and asking under a former domain would not help.

The measure is whole-month coverage, not whether the source returned any rows. The month containing a switchover is served partly by each domain, so the current one returns a day or two and every dataset looks like it returned data while covering almost none of the month. Judging by "returned something" skips exactly the month that needs both domains. Records from the two requests are unioned before coverage is judged again, so a straddling month resolves as complete.

No switchover date is encoded. Trying the former domain only when the current one came back empty means nothing can fall out of step with what the source actually serves, and a month straddling a boundary needs no special handling.

### Month Boundaries

ENTSO-E requests are padded either side of the month so Silver can reconstruct complete local-timezone calendar dates at the edges.

Only dates inside the plain calendar month count toward completeness. The overlap is harmless because the Bronze merge key makes the write idempotent.

## Authentication

The blob bucket is reached with a long-lived bucket token, exchanged at the provider's credential endpoint for temporary S3 credentials.

Those credentials expire in minutes, far shorter than a single invocation. They must therefore be minted on demand and re-minted ahead of expiry for the life of the invocation, not once at startup.

The bucket token and the ENTSO-E token come from the environment. Neither is ever committed, passed as a job parameter, or written to a run's parameter list.

## Loading Into Bronze

Run once, by hand, after the ingestion reports that every month is accounted for.

The load is two jobs, not one: a Bronze load, and a Silver/Gold rebuild started separately once the Bronze load's verification has been read. Each of the three stages involved fails differently — staging is a sequence of cheap, resumable downloads, the merge is expensive and wants to be atomic, and the rebuild is the single most expensive run in the project. Running them together means a failure in any one costs the work of all three, which on a bounded compute allowance is the difference between retrying a step and losing a day.

### Staging

Every Parquet file is copied, byte for byte, from blob storage onto a Unity Catalog volume. Nothing is parsed and only one file is held in memory at a time, so the stage's cost is bounded by network time rather than by the size of the dataset.

The files are copied in rather than referenced where they lie because the bucket is not a Unity Catalog external location.

Spark then reads the Parquet off the volume itself. The driver never holds a row. Materialising a decade of history as Python objects to hand to `createDataFrame` would move the entire dataset through one process, and the largest single month would be the peak memory of the whole load.

A file is written under a `.part` suffix and renamed into place only when complete. A file at its final path is therefore always whole, which is what makes "skip what is already staged" a safe resumption rule rather than a way to inherit a truncated download.

Staged paths drop the `year=`/`month=` syntax the blob keys use. Spark reads a directory tree of that shape as a Hive-partitioned dataset and synthesises a `year` column from the directory name, which would no longer match the Bronze schema.

### Merging

A batch is one source-year. The merge, not the read, is the expensive half: a year is small enough to merge in one operation while still being a unit worth resuming from, and it replaces one merge per month with one per year.

Years are merged oldest first, so a pass interrupted part-way leaves Bronze holding a contiguous history rather than islands.

The merge is the same deterministic-schema path the daily pipeline uses, keyed on the same Bronze business key. A period already present is therefore updated in place rather than duplicated, which is what makes the load safe to re-run and safe to run against Bronze tables that already hold overlapping history. Rows Bronze holds that the staged files do not are left untouched — a MERGE never deletes.

Where the two overlap, the staged row wins and the Bronze row's `ingestion_timestamp` is rewritten. Both sides read the same source for the same window, so the values are otherwise identical, and Bronze is raw landing data.

Which batches have been merged is recorded in explicit state on the volume, never inferred from Bronze row counts: Bronze also holds rows the daily pipeline wrote, so a count cannot distinguish "this batch was merged" from "some of these rows arrived another way". Because the merge is idempotent, this state is an optimisation rather than a correctness mechanism — it exists so that retrying one year does not re-merge a decade.

State is saved after every batch. A pass that dies after merging eight years must not lose the record of those eight.

A batch that fails to merge is recorded and the pass continues — one bad year must not stop the rest of a decade. The task then fails rather than reporting success with a gap.

### Verifying

The load reports what it merged; the verification checks the result independently, by asking whether any row on the volume has a merge key absent from Bronze.

That is a left-anti join, not a row count. Counts cannot answer it: Bronze is legitimately larger than the staged set, because the daily pipeline wrote some of its rows, so two numbers failing to match proves nothing in either direction. A missing key is unambiguous.

A missing Bronze table is an error, not a pass. An absent table means the load never ran, and that must not read as "nothing is missing".

### After The Load

Silver and Gold are rebuilt over the widened history and the serving layer republished, as a separate job started by hand. The verification output is meant to be read by a person first: a decade of Silver plus a full dbt build should not start on a Bronze that is quietly short of a year.

What that job publishes to PostgreSQL is scoped separately. PostgreSQL is a serving layer, not storage, and the full history is far larger than it should hold.

`dim_date` must cover the full historical range, or fact rows will have no matching dimension row and the Gold relationship tests will fail.

The current month-to-date is not covered by this layer. It is closed by one run of the daily workflow in `reprocess` mode for that range.

## Observability

Work that is discarded must be visible in the run log, not only in a result object a later reader might inspect.

This applies in particular to combinations skipped because the circuit breaker opened. One open breaker can account for the overwhelming majority of a run's failures, and a run that discards most of a month must not look the same as one that attempted it.

A failure's HTTP status is never dropped in favour of a complaint about the response body. A gateway or throttling layer in front of a source answers with HTML or plain text, and the status is the one fact that explains the failure.

Row counts reported by an invocation are rows that invocation wrote, not rows the destination file now holds. Reporting the file total would let a run that re-fetched one country claim a whole month's rows as its own.

## Acceptance Criteria

1. One invocation resolves exactly one year and exits.
2. The walk moves backwards from the most recent unresolved year to January 2015.
3. Only complete calendar months are in scope.
4. Progress is read from and written to explicit state, never inferred from stored data.
5. A partially covered month is retryable, never treated as done.
6. An empty month is only `unavailable` when the source positively confirmed it.
7. A combination that fails three times escalates to `given_up` and stops blocking the walk, and the invocation that abandons it reports a failure rather than a clean run.
7a. An invocation that leaves combinations merely `failed` reports success, and warns about them.
8. A `given_up` combination can be returned to the walk without editing code.
9. A re-attempt fetches only what is unresolved.
10. Progress is saved after every month, so an interrupted invocation keeps its completed months.
11. Parquet schemas are derived from the Bronze field definitions, not declared separately.
12. An attempt producing no rows leaves an existing file untouched.
13. Re-running a year produces the same files, not additional ones.
14. Blob credentials are re-minted as needed for the life of an invocation.
15. Concurrent invocations are prevented, regardless of how often a dispatch arrives.
16. Loading the same period into Bronze twice does not duplicate records, and never deletes rows Bronze alone holds.
17. A single unreadable file stops neither the staging pass nor the merge pass, and the task still fails.
18. The ingestion reuses the daily workflow's ingestion modules rather than reimplementing them.
19. Combinations skipped by an open circuit breaker appear in the run log.
20. A non-success HTTP status is reported even when the response body cannot be interpreted.
21. Reported row counts are rows written by that invocation.
22. A country reporting nothing for a whole month is retried under its former bidding-zone domain, where one is known.
21. Staging is resumable: an already-staged file is skipped, and a partially written one is not mistaken for a complete one.
22. Spark reads the staged Parquet directly; no row passes through the driver.
23. Staged paths cannot be read by Spark as Hive partitions.
24. Bronze is merged one source-year at a time, oldest first, with progress recorded after each.
25. The Bronze load and the Silver/Gold rebuild are separate jobs with no dependency between them.
26. The load is verified by merge-key absence, not by comparing row counts.
