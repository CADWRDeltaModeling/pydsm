# pydsm — `diff`

Structural and time-series comparison of two DSM2 **input** studies — answers the
question *"why do these two studies' outputs differ?"* by comparing their Hydro
echo files (`.inp`).

Use this **after** `pydsm compare-dss` or `dsm2ui calib postpro` has shown you
*that* (and roughly *where*) two studies' outputs diverge. `diff` then tells you
*what changed in the model setup* that could explain it — channel geometry, gates,
boundary conditions, operating rules, etc.

See also:
- [README-compare-dss.md](README-compare-dss.md) — quick two-file output comparison
- [../dsm2ui/README-postpro.md](../dsm2ui/README-postpro.md) — rich multi-study
  output comparison and plotting
- [../dsm2ui/README-integrated-comparison.md](../dsm2ui/README-integrated-comparison.md) —
  the full "output diff → input diff" workflow

---

## What it does

`pydsm diff` loads both studies' Hydro echo files via `DSM2Diff(echo_a, echo_b)`
and produces a `FullReport` with two kinds of comparison:

1. **Static table diffs** — row-level differences in DSM2's structural input
   tables (added / removed / changed rows), matched by a per-table primary key.
2. **Time-series diffs** — for DSS-backed input tables (boundary conditions,
   operating-rule time series, etc.), computes RMSE / bias / NSE / Pearson r
   between the two studies' underlying DSS series for each named entry.

It does **not** compare model *output* (DSS results) — for that, use
`pydsm compare-dss` or `dsm2ui calib postpro`.

## Which tables are compared

### Static tables (row-level added/removed/changed)

Matched on a primary key per table (from `TABLE_KEYS`):

| Table | Key columns |
|---|---|
| `CHANNEL` | `CHAN_NO` |
| `XSECT` | `CHAN_NO`, `DIST` |
| `XSECT_LAYER` | `CHAN_NO`, `DIST`, `ELEV` |
| `RESERVOIR` | `NAME` |
| `RESERVOIR_VOL` | `RES_NAME`, `ELEV` |
| `RESERVOIR_CONNECTION` | `RES_NAME`, `NODE` |
| `GATE` | `NAME` |
| `GATE_WEIR_DEVICE` / `GATE_PIPE_DEVICE` | `GATE_NAME`, `DEVICE` |
| `TRANSFER` | `NAME` |
| `CHANNEL_IC` | `CHAN_NO`, `DISTANCE` |
| `RESERVOIR_IC` | `RES_NAME` |
| `BOUNDARY_STAGE` / `BOUNDARY_FLOW` | `NAME` |
| `SOURCE_FLOW` / `SOURCE_FLOW_RESERVOIR` | `NAME` |
| `INPUT_GATE` | `GATE_NAME`, `DEVICE`, `VARIABLE` |
| `INPUT_TRANSFER_FLOW` | `TRANSFER_NAME` |
| `OPERATING_RULE` / `OPRULE_EXPRESSION` / `OPRULE_TIME_SERIES` | `NAME` |
| `OUTPUT_CHANNEL` / `OUTPUT_RESERVOIR` / `OUTPUT_GATE` | `NAME`, `VARIABLE` |
| `ENVVAR` / `SCALAR` | `NAME` |
| `IO_FILE` | `MODEL`, `TYPE` |

### DSS-backed time-series tables

Default set compared (`-t/--tables`, or restrict via `-T/--table`):

| Table | Default | Name column |
|---|---|---|
| `BOUNDARY_FLOW` | ✅ | `NAME` |
| `BOUNDARY_STAGE` | ✅ | `NAME` |
| `OPRULE_TIME_SERIES` | ✅ | `NAME` |
| `SOURCE_FLOW` | optional | `NAME` |
| `SOURCE_FLOW_RESERVOIR` | optional | `NAME` |
| `INPUT_GATE` | optional | synthesized `GATE_NAME/DEVICE/VARIABLE` |
| `INPUT_TRANSFER_FLOW` | optional | `TRANSFER_NAME` |

Use `--all-tables` to compare every table in this list, or `--tables` /
`-t TABLE` (repeatable) to pick a specific subset.

## Variable → table guidance

When following up on an output difference, use this as a starting point for which
input tables are worth diffing:

| Output variable that differs | Input tables worth checking |
|---|---|
| `FLOW` | `CHANNEL` (geometry, Manning), `BOUNDARY_FLOW`, `GATE`/`INPUT_GATE`, `TRANSFER`/`INPUT_TRANSFER_FLOW` |
| `STAGE` | `CHANNEL`, `BOUNDARY_STAGE`, `RESERVOIR`/`RESERVOIR_CONNECTION`, `GATE` |
| `EC` | `CHANNEL` (DISPERSION), `BOUNDARY_STAGE`, `OPERATING_RULE`/`OPRULE_TIME_SERIES`, `SOURCE_FLOW` |

## CLI

```
pydsm diff ECHO_A ECHO_B [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `ECHO_A`, `ECHO_B` | *(required)* | Paths to the two Hydro echo `.inp` files |
| `-T / --table` | — | Restrict output to this table (repeatable). If the table is TS-backed, its DSS metric comparison runs automatically |
| `-t / --tables` | `BOUNDARY_FLOW BOUNDARY_STAGE OPRULE_TIME_SERIES` | DSS-backed tables to compare for time-series data (repeatable) |
| `--all-tables` | off | Compare DSS data in all known TS-backed tables (overrides `--tables`) |
| `--timewindow` | run-period intersection | Comparison window, e.g. `"01JAN2020 0000 - 01JAN2022 0000"` |
| `--threshold` | `0.01` | RMSE below this value is not reported as a difference |
| `--max-ts` | `25` | Skip DSS loading for tables with more rows than this |
| `--force` | off | Load DSS data even when a table exceeds `--max-ts` rows |
| `-o / --output` | stdout | Write the text report to this file instead of stdout |
| `--outdir` | — | Directory for CSV output files. Omit to suppress CSV output |
| `--no-csv` | off | Suppress CSV output (equivalent to omitting `--outdir`) |

```bash
pydsm diff base/hydro_echo.inp variant/hydro_echo.inp \
  -T CHANNEL -T BOUNDARY_FLOW \
  --timewindow "01JAN2020 0000 - 01JAN2022 0000" \
  --threshold 0.05 \
  --outdir diff_output/ \
  -o diff_report.txt
```

## Time window handling

- **Default**: intersection of both studies' `run_start_date` / `run_end_date`
  (read from each echo file's `SCALAR` section).
- **Override**: `--timewindow "01JAN2020 0000 - 01JAN2022 0000"`.
- Raises `ValueError` if the two studies' run periods don't overlap at all.
- The `2400`-means-midnight-next-day DSM2 convention is handled automatically.

## Output formats

### Terminal report (or `-o/--output` file)

```
DSM2 Study Diff
A: /path/to/a.inp
B: /path/to/b.inp
Comparison window: 01JAN2020 0000 - 01JAN2022 0000
=====================================================

[STATIC TABLES]  3 have differences, 12 are identical
  Identical: CHANNEL, BOUNDARY_STAGE, ...

CHANNEL  —  +2 added, -1 removed, ~5 changed
───────────────────────────────────────────────
  Added (2 rows — present in study_b only):
    CHAN_NO  LENGTH  MANNING  ...
         999  15000    0.035
  ...

[TIME SERIES DATA COMPARISONS]
───────────────────────────────────────────────
BOUNDARY_FLOW
  Summary (14 entries compared):
    name  | path_match | rmse | bias | n_points | skipped
    sac   |   success  | 0.02 | 1.5  |   8760   |    False
    ...
```

### CSV output (`--outdir`)

| File | Contents |
|---|---|
| `{table}_added.csv` | Rows present only in study B |
| `{table}_removed.csv` | Rows present only in study A |
| `{table}_changed.csv` | Rows with the same key but different values (side-by-side) |
| `{table}_ts_summary.csv` | Per-entry RMSE / bias / NSE / Pearson r / mean values / point counts |
| `{table}_ts_missing.csv` | Entries present in only one study |
| `{table}_{entry_name}_diff.csv` | Detailed a/b/diff series for entries exceeding `--threshold` |

## Python API

```python
from pydsm.analysis.dsm2diff import DSM2Diff

diff = DSM2Diff("base/hydro_echo.inp", "variant/hydro_echo.inp")
report = diff.compare(tables=["BOUNDARY_FLOW", "OPRULE_TIME_SERIES"], threshold=0.05)
report.print_report()
report.to_csv("diff_output/")
```

`report.static_diffs` is a dict of `StaticDiff(table, added, removed, changed)`;
`report.ts_diffs` is a dict of `TSDiff(table, summary, missing, diff_series)`.

## Limitations

- Compares **Hydro** echo files only (not qual/gtm echo files directly) — but
  since qual/gtm boundary/operating-rule inputs are typically the same structures,
  most relevant comparisons are still covered.
- Large DSS-backed tables (> `--max-ts` rows) are skipped by default for
  performance; use `--force` to load them anyway.
- Does not itself compute output-level metrics (RMSE between model results) —
  pair it with `pydsm compare-dss` or `dsm2ui calib postpro` for that.
