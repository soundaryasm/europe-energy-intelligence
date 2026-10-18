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

Runs are scheduled, not run as a single long job, and spacing must exceed the worst-case duration of one year rather than its typical duration.

A year is roughly 2,700 ENTSO-E requests. Typically that is around fifteen minutes. Every request carries a bounded retry with exponential backoff and a read timeout, so a degraded upstream can stretch a year past an hour.

Concurrent invocations are not permitted. ENTSO-E rate-limits per token as well as per IP, and this layer holds the only token; two invocations fetching at once would share that budget and risk the ban that follows exceeding it.

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

### Month Boundaries

ENTSO-E requests are padded either side of the month so Silver can reconstruct complete local-timezone calendar dates at the edges.

Only dates inside the plain calendar month count toward completeness. The overlap is harmless because the Bronze merge key makes the write idempotent.

## Authentication

The blob bucket is reached with a long-lived bucket token, exchanged at the provider's credential endpoint for temporary S3 credentials.

Those credentials expire in minutes, far shorter than a single invocation. They must therefore be minted on demand and re-minted ahead of expiry for the life of the invocation, not once at startup.

The bucket token and the ENTSO-E token come from the environment. Neither is ever committed, passed as a job parameter, or written to a run's parameter list.

## Loading Into Bronze

Run once, by hand, after the ingestion reports that every month is accounted for.

Every Parquet file is read and merged into its Bronze table, oldest period first. Oldest first so a pass interrupted part-way leaves Bronze holding a contiguous history rather than islands.

Files are fetched over HTTPS into the driver and read from there, rather than registered as an external table: the bucket is not a Unity Catalog external location, and a month's file is a few megabytes, so the download costs less than the configuration it would otherwise need.

The merge is the same deterministic-schema path the daily pipeline uses, keyed on the same Bronze business key. Loading a month that is already present therefore updates those rows in place instead of duplicating them, which is what makes the load safe to re-run and safe to run against Bronze tables that already hold overlapping history.

A file that fails to load is recorded and the pass continues — one unreadable month must not stop the rest of a decade. The task then fails rather than reporting success with a gap.

### After The Load

The load job rebuilds Silver and Gold over the widened history and republishes the serving layer.

`dim_date` must cover the full historical range, or fact rows will have no matching dimension row and the Gold relationship tests will fail.

The current month-to-date is not covered by this layer. It is closed by one run of the daily workflow in `reprocess` mode for that range.

## Acceptance Criteria

1. One invocation resolves exactly one year and exits.
2. The walk moves backwards from the most recent unresolved year to January 2015.
3. Only complete calendar months are in scope.
4. Progress is read from and written to explicit state, never inferred from stored data.
5. A partially covered month is retryable, never treated as done.
6. An empty month is only `unavailable` when the source positively confirmed it.
7. A combination that fails three times escalates to `given_up` and stops blocking the walk.
8. A `given_up` combination can be returned to the walk without editing code.
9. A re-attempt fetches only what is unresolved.
10. Progress is saved after every month, so an interrupted invocation keeps its completed months.
11. Parquet schemas are derived from the Bronze field definitions, not declared separately.
12. An attempt producing no rows leaves an existing file untouched.
13. Re-running a year produces the same files, not additional ones.
14. Blob credentials are re-minted as needed for the life of an invocation.
15. Concurrent invocations are prevented.
16. Loading the same month into Bronze twice does not duplicate records.
17. A single unreadable file does not stop the load pass, and the task still fails.
18. The ingestion reuses the daily workflow's ingestion modules rather than reimplementing them.
