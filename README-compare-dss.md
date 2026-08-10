# pydsm — `compare-dss`

Quick, zero-setup numeric comparison of two DSM2 (or any HEC-DSS) output files.

Use this when you have **exactly two DSS files** and want a fast goodness-of-fit
check — no location CSVs, no config files, no observed-data setup. If you need to
overlay **more than two** studies, plot time series, or bring in observed field
data, use `dsm2ui calib postpro` instead (see
[../dsm2ui/README-postpro.md](../dsm2ui/README-postpro.md)). If the numbers here
show a difference and you want to know *why*, follow up with `pydsm diff` on the
two studies' input echo files (see [README-diff.md](README-diff.md)).

See also: [README-integrated-comparison.md](../dsm2ui/README-integrated-comparison.md)
for the full workflow that chains all three tools together.

---

## What it does

`compare-dss` matches the two DSS files' catalogs on **B-part (station) and C-part
(variable)**, aligns the resulting pairs of time series, and computes goodness-of-fit
metrics for every matching pathname. Metrics are written to a CSV; optionally, an
HTML overlay plot is generated for every pathname whose metric exceeds a threshold.

It does **not** inspect DSM2 input files, channel geometry, or operating rules —
it is purely an output-vs-output numeric comparison.

## When to use it

- You have a baseline DSS file and an alternative/variant DSS file and want a fast
  pass/fail-style check of what changed, station by station.
- You don't want to prepare a location CSV or a postpro config — `compare-dss`
  works directly off DSS path catalogs.
- You want CSV output suitable for scripting/CI (e.g. flag studies whose RMSE at
  key stations exceeds a tolerance).

## When *not* to use it

- You need to overlay 3+ studies, or compare against observed field data with rich
  time-series/scatter/tidal plots → use `dsm2ui calib postpro` (see
  [README-postpro.md](../dsm2ui/README-postpro.md), including the no-observed
  "quick-compare" `setup-compare` mode for baseline-vs-alternative planning studies).
- You want to know *which DSM2 input changed* to explain a difference → use
  `pydsm diff` (see [README-diff.md](README-diff.md)).

## CLI

```
pydsm compare-dss DSSFILE1 DSSFILE2 [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DSSFILE1`, `DSSFILE2` | *(required)* | Two DSS files to compare |
| `--cpart` | — | Filter to paths matching this C-part string (e.g. `EC`) |
| `--threshold` | `0.001` | Metric value above which a path is flagged as different |
| `--threshold-metric` | `rmse` | Metric used for threshold check — see the metric table below |
| `--metricsfile` | `compare_dss_metrics_diff.csv` | Output metrics CSV filename |
| `--time-window` | — | Comparison window, e.g. `"01JAN1990 - 01OCT1991"` |
| `--threshold-plots` | `False` | Write HTML plots for paths that exceed the threshold |

```bash
pydsm compare-dss base.dss variant.dss --cpart EC --time-window "01JAN2020 - 01JAN2022"
```

Note: the underlying `dssutils.compare_dss()` function also accepts a `godin`
parameter (apply a Godin tidal filter before comparing), but it is not currently
exposed as a CLI flag — only reachable via the Python API.

## Metrics computed

Eight goodness-of-fit metrics (from `pydsm.functions.tsmath`), one row per matching
DSS pathname:

| Metric | Ideal value | Notes |
|---|---|---|
| `mean_error` | 0.0 | Signed mean difference |
| `nmean_error` | 0.0 | Normalized mean error |
| `mse` | 0.0 | Mean squared error |
| `nmse` | 0.0 | Normalized mean squared error |
| `rmse` | 0.0 | Root mean squared error (default threshold metric) |
| `nrmse` | 0.0 | Normalized RMSE |
| `nash_sutcliffe` | 1.0 | Nash-Sutcliffe efficiency |
| `percent_bias` | 0.0 | Percent bias |

## How matching works

1. Both DSS files are opened with `pyhecdss.DSSFile` and cataloged.
2. Catalogs are merged on matching **B-part** and **C-part** — pathnames present
   in only one file are skipped (not reported as differences).
3. If `--cpart` is given, only paths matching that C-part are compared (e.g.
   `--cpart EC` to compare only electrical conductivity).
4. If `--time-window` is given, both series are restricted to that window before
   computing metrics (split on `" - "`).
5. Regular and irregular (`IR*` E-part) time series are both supported via an
   internal `_read_ts()` helper that auto-detects the series type.
6. Metrics are computed on the aligned pair and appended as a row to the output
   CSV. If `--threshold-plots` is set, an HTML overlay plot is written for every
   pathname whose `--threshold-metric` value exceeds `--threshold`.

## Python API

```python
from pydsm.analysis import dssutils

dssutils.compare_dss(
    "base.dss", "variant.dss",
    threshold=0.05,
    threshold_metric="rmse",
    time_window="01JAN2020 - 01JAN2022",
    cpart="EC",
    godin=True,               # only reachable via the Python API, not the CLI
    metricsfile="ec_diff.csv",
    threshold_plots=True,
)
```

## Limitations

- Exactly two DSS files — no built-in support for N-way (baseline + multiple
  alternatives) comparison. For that, use `dsm2ui calib postpro`.
- No structural/input comparison — it cannot tell you *why* two outputs differ,
  only *that* and *by how much*. Pair it with `pydsm diff` for the "why".
