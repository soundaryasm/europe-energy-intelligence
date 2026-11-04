# Historical backfill record

Outcome of the one-off historical ingestion described in
`specs/010-historical-blob-backfill.md`. The walk covered 2015-01 to
2026-10 and completed on 2026-11-04.

The blob bucket it wrote to has since been emptied and the workflow
disabled. These files are the record of what was fetched.

## Files

| File | Contents |
|---|---|
| `manifest.json` | Per-month outcome for every source/country/dataset |
| `manifest.backup-pre-de-reset.json` | The same, before the DE domain reset |
| `load_state.json` | Per-source-year file list and byte sizes merged into Bronze |
| `blob-archive-checksums.json` | MD5 and byte count for each of the 297 blob objects |

## Result

142 months, 8,520 source/country/dataset combinations:

| Status | Count |
|---|---|
| `success` | 8,493 |
| `given_up` | 26 |
| `unavailable` | 1 |

295 Parquet files, 232,296,687 bytes, merged into Bronze as 36,502,077
ENTSO-E rows and verified row-by-row against the staged files.

## Unresolved combinations

| Month | Combination | Status |
|---|---|---|
| 2015-01 | `entsoe:BE:price` | `given_up` |
| 2015-01 | `entsoe:DE:price` | `given_up` |
| 2015-01 | `entsoe:FR:price` | `given_up` |
| 2015-01 | `entsoe:NL:price` | `given_up` |
| 2015-01 | `entsoe:PL:price` | `given_up` |
| 2015-01 | `entsoe:RO:generation` | `given_up` |
| 2015-01 | `entsoe:RO:load` | `given_up` |
| 2015-01 | `entsoe:SK:generation` | `given_up` |
| 2015-02 | `entsoe:PL:price` | `given_up` |
| 2015-03 | `entsoe:PL:price` | `given_up` |
| 2015-05 | `entsoe:PL:price` | `given_up` |
| 2015-06 | `entsoe:SK:generation` | `given_up` |
| 2015-09 | `entsoe:IE:price` | `given_up` |
| 2015-09 | `entsoe:PL:price` | `given_up` |
| 2015-10 | `entsoe:IE:price` | `given_up` |
| 2015-10 | `entsoe:PL:price` | `given_up` |
| 2015-11 | `entsoe:PL:price` | `given_up` |
| 2015-12 | `entsoe:RO:price` | `given_up` |
| 2016-01 | `entsoe:DE:load` | `unavailable` |
| 2016-04 | `entsoe:IE:load` | `given_up` |
| 2016-04 | `entsoe:PL:price` | `given_up` |
| 2018-09 | `entsoe:IE:price` | `given_up` |
| 2021-09 | `entsoe:IE:load` | `given_up` |
| 2022-10 | `entsoe:IE:generation` | `given_up` |
| 2022-12 | `entsoe:IE:load` | `given_up` |
| 2024-03 | `entsoe:IE:generation` | `given_up` |
| 2026-01 | `entsoe:FR:generation` | `given_up` |

The 2015-01 cluster reflects the ENTSO-E Transparency Platform going
live on 5 January 2015; no publisher had a full month to report.

## Parquet

The 295 Parquet files are not in this repository. They remain on the
Unity Catalog volume `/Volumes/workspace/default/historical` and in an
offline archive whose checksums are in `blob-archive-checksums.json`.
Re-staging them is not required to query any layer — Bronze holds the
merged result.
