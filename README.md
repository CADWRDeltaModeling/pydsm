# pydsm

For working with DSM2 input (`.inp`) and output (`.h5` and `.dss`) with Python (pandas).

- Free software: MIT license
- Documentation: https://cadwrdeltamodeling.github.io/pydsm

---

## Features

- Reads DSM2 HDF5 tidefiles into pandas DataFrames
  - [Hydro tidefile](https://cadwrdeltamodeling.github.io/pydsm/pydsm.output.hydroh5.html): time series data, input tables, and geometry tables
  - [Qual tidefile](https://cadwrdeltamodeling.github.io/pydsm/pydsm.output.qualh5.html): time series data and input tables
  - [GTM tidefile](https://cadwrdeltamodeling.github.io/pydsm/pydsm.output.gtmh5.html): constituent concentration data
- [Reads and writes DSM2 echo/input files](https://cadwrdeltamodeling.github.io/pydsm/pydsm.input.parser.html) into and from pandas DataFrames
- Working with DSS time series files: extract, compare, copy, extend, and convert data

---

## Installation

### Prerequisites

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
- The `cadwr-dms` conda channel is required for `pyhecdss` and `vtools3`

### Conda (recommended)

```bash
conda env create -f environment.yml
conda activate pydsm
```

This installs the core runtime plus visualization extras (`geopandas`, `shapely`).

### pip

```bash
pip install pydsm
```

Install optional extras as needed:

```bash
pip install "pydsm[viz]"   # geopandas, shapely
pip install "pydsm[dev]"   # test + docs tools
```

---

## Commands

```
pydsm --help
```

### DSS file utilities

#### `extract-dss`

Extract time series from a DSS file, with optional C-part filtering, Godin tidal filter, and resampling.

```
pydsm extract-dss DSSFILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DSSFILE` | *(required)* | Input DSS file |
| `-o / --outfile` | `out.gz` | Output file path. Extension determines format: `.gz`/`.zip`/`.bz2` → pickle, `.h5` → HDF5, `.dss` → DSS |
| `--cpart` | — | Filter to paths matching this C-part string (e.g. `EC`) |
| `-godin / --godin-filter` | off | Apply Godin tidal filter before writing |
| `-davg / --daily-average` | off | Average to daily values |
| `-dmax / --daily-max` | off | Daily maximum |
| `-dmin / --daily-min` | off | Daily minimum |
| `-mavg / --monthly-average` | off | Monthly average |

```bash
pydsm extract-dss model_output.dss -o ec_daily.gz --cpart EC -davg
```

---

#### `compare-dss`

Compare two DSS files on matching B/C-part pathnames and write goodness-of-fit metrics to a CSV.

```
pydsm compare-dss DSSFILE1 DSSFILE2 [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DSSFILE1`, `DSSFILE2` | *(required)* | Two DSS files to compare |
| `--cpart` | — | Filter to paths matching this C-part string |
| `--threshold` | `0.001` | Metric value above which a path is flagged as different |
| `--threshold-metric` | `rmse` | Metric used for threshold check. Choices: `mean_error`, `nmean_error`, `mse`, `nmse`, `rmse`, `nrmse`, `nash_sutcliffe`, `percent_bias` |
| `--metricsfile` | `compare_dss_metrics_diff.csv` | Output metrics CSV filename |
| `--time-window` | — | Comparison window, e.g. `"01JAN1990 - 01OCT1991"` |
| `--threshold-plots` | `False` | Write HTML plots for paths that exceed the threshold |

```bash
pydsm compare-dss base.dss variant.dss --cpart EC --time-window "01JAN2020 - 01JAN2022"
```

---

#### `copy-all-dss`

Copy all pathnames from one DSS file to another.

```
pydsm copy-all-dss FROM_FILE TO_FILE
```

```bash
pydsm copy-all-dss source.dss destination.dss
```

---

#### `csv-to-dss`

Convert a CSV time series file to a DSS file.

```
pydsm csv-to-dss CSV_FILE DSS_FILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `CSV_FILE`, `DSS_FILE` | *(required)* | Input CSV and output DSS paths |
| `--index_col` | `0` | Column index to use as the datetime index |
| `--apart` | `A` | DSS A-part |
| `--bpart` | `F` | DSS B-part (location) |
| `--fpart` | `F` | DSS F-part (version/study label) |
| `--unit` | `UNK` | Physical unit string |
| `--period_type` | `INST-VAL` | Period type (e.g. `INST-VAL`, `PER-AVER`) |
| `--multiplier` | `1.0` | Scale factor applied to all values |
| `--resample_to` | `15T` | Pandas resample frequency (e.g. `15T`, `1H`, `1D`) |

```bash
pydsm csv-to-dss observed_ec.csv observed_ec.dss --bpart RSAC075 --unit uS/cm --resample_to 15T
```

---

#### `extend-dss-ts`

Extend time series in a DSS file by appending a shifted copy.

```
pydsm extend-dss-ts DSS_FILENAME DSS_EXT_FILENAME [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DSS_FILENAME` | *(required)* | Source DSS file |
| `DSS_EXT_FILENAME` | *(required)* | Output DSS file with extended data |
| `--days` | `366` | Number of days to shift the appended copy |
| `--pathfilter` | `///////` | HECDSS path filter (HEC wildcard syntax) |

```bash
pydsm extend-dss-ts boundary.dss boundary_extended.dss --days 366
```

---

#### `repeating create`

Create a repeating annual time series from a CSV template year and write it to a DSS file.

```
pydsm repeating create DATAFILE --input-file FILE --path PATH --units UNITS [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DATAFILE` | *(required)* | Output DSS file to write repeating series into |
| `--input-file` | *(required)* | CSV file containing the template year data |
| `--path` | *(required)* | Full HECDSS path to write (e.g. `/A/STATION/FLOW//1DAY/F/`) |
| `--units` | *(required)* | Physical unit string |
| `--period-type` | `PER-AVER` | DSS period type |

```bash
pydsm repeating create output.dss --input-file template_year.csv --path "/TEMPLATE/STATION/FLOW//1DAY/REP/" --units CFS
```

---

#### `repeating extend`

Extend an existing repeating time series in a DSS file forward to a given end year.

```
pydsm repeating extend DATAFILE --cpart CPART --end-year YEAR
```

| Argument / Option | Default | Description |
|---|---|---|
| `DATAFILE` | *(required)* | DSS file containing the repeating series |
| `--cpart` | *(required)* | C-part filter to identify the datasets to extend |
| `--end-year` | *(required)* | Target end year (integer) |

```bash
pydsm repeating extend output.dss --cpart FLOW --end-year 2030
```

---

#### `calc-netcd`

Calculate aggregated Net Channel Depletion (NetCD = DIV-FLOW − DRAIN-FLOW + SEEP-FLOW) from a DSS file.

```
pydsm calc-netcd DSSFILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `DSSFILE` | *(required)* | Input DSS file containing diversion, drain, and seepage paths |
| `--bpart` | — | B-part to include (repeatable, e.g. `--bpart BBID --bpart 12345`). Defaults to all numeric node IDs. |
| `--bpart-file` | — | Text file with one B-part per line |
| `--no-seepage` | off | Exclude seepage: NetCD = DIV − DRAIN only |
| `--epart` | `1DAY` | Time interval E-part (e.g. `1DAY`, `1MON`) |
| `-o / --output` | `netcd.csv` | Output CSV file path |

```bash
pydsm calc-netcd dcd_nodes.dss --bpart BBID --epart 1MON -o netcd_monthly.csv
```

---

### DSM2 input / echo files

#### `pretty-print-input`

Reformat (pretty-print) a DSM2 `.inp` echo file, normalising whitespace and column alignment.

```
pydsm pretty-print-input INPUT_FILE [OUTPUT_FILE]
```

| Argument | Default | Description |
|---|---|---|
| `INPUT_FILE` | *(required)* | DSM2 `.inp` echo file to reformat |
| `OUTPUT_FILE` | `<basename>.pretty.inp` | Output file path |

```bash
pydsm pretty-print-input hydro_echo.inp
```

---

#### `create-dsm2-input-for-cd`

Generate a DSM2 `.inp` boundary-condition file for consumptive demand from a DSS file.

```
pydsm create-dsm2-input-for-cd DSS_FILENAME DSM2_INPUT_FILENAME FILE_FIELD_STRING
```

| Argument | Description |
|---|---|
| `DSS_FILENAME` | DSS file containing the consumptive demand time series |
| `DSM2_INPUT_FILENAME` | Output `.inp` file to write |
| `FILE_FIELD_STRING` | String used as the `FILE` field value in the generated input table |

```bash
pydsm create-dsm2-input-for-cd dcd_2020.dss dcd_boundary.inp "\${BNDRYINPUT}"
```

---

#### `chan-orient`

Generate a channel orientation (angle) `.inp` file from a GeoJSON channel geometry and a Hydro echo file.

```
pydsm chan-orient CHANNEL_LINE_GEOJSON_FILE HYDRO_ECHO_FILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `CHANNEL_LINE_GEOJSON_FILE` | *(required)* | GeoJSON file of channel centrelines |
| `HYDRO_ECHO_FILE` | *(required)* | DSM2 Hydro echo `.inp` file |
| `--channel_orient_file` | `channel_orient.inp` | Output orientation `.inp` file path |

```bash
pydsm chan-orient dsm2_channels.geojson hydro_echo.inp --channel_orient_file channel_orient.inp
```

---

#### `diff`

Compare two DSM2 studies by their Hydro echo files. Reports structural differences in all input tables and computes RMSE/bias for DSS-backed time-series inputs.

```
pydsm diff ECHO_A ECHO_B [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `ECHO_A`, `ECHO_B` | *(required)* | Paths to the two Hydro echo `.inp` files |
| `-t / --tables` | `BOUNDARY_FLOW BOUNDARY_STAGE OPRULE_TIME_SERIES` | DSS-backed tables to compare for time-series data (repeatable) |
| `--all-tables` | off | Compare DSS data in all known TS-backed tables |
| `--timewindow` | run-period intersection | Comparison window, e.g. `"01JAN2020 0000 - 01JAN2022 0000"` |
| `--threshold` | `0.01` | RMSE below this is not reported as a difference |
| `--max-ts` | `25` | Skip DSS loading for tables with more rows than this |
| `--force` | off | Load DSS data even when a table exceeds `--max-ts` rows |
| `--outdir` | `.` | Directory for CSV output files |
| `--no-csv` | off | Print report to terminal only; suppress CSV output |

```bash
pydsm diff base/hydro_echo.inp variant/hydro_echo.inp --timewindow "01JAN2020 0000 - 01JAN2022 0000" --outdir diff_output/
```

---

### HDF5 tidefile utilities

#### `slice-hydro`

Slice a Hydro HDF5 tidefile to a time window and write a new tidefile.

```
pydsm slice-hydro INFILE OUTFILE STIME ETIME
```

| Argument | Description |
|---|---|
| `INFILE` | Input Hydro HDF5 tidefile |
| `OUTFILE` | Output (sliced) Hydro HDF5 tidefile |
| `STIME` | Start datetime string, e.g. `1990-01-10` |
| `ETIME` | End datetime string, e.g. `1990-03-31` |

```bash
pydsm slice-hydro historical.h5 sliced.h5 1990-01-10 1990-03-31
```

---

#### `update-hydro-tidefile-with-inp`

Patch an input table inside a Hydro HDF5 tidefile from a DSM2 `.inp` file.

```
pydsm update-hydro-tidefile-with-inp HYDRO_TIDEFILE INPUT_FILE [TIDEFILE_PATH] [TABLE_NAME]
```

| Argument | Default | Description |
|---|---|---|
| `HYDRO_TIDEFILE` | *(required)* | HDF5 tidefile to update (modified in place) |
| `INPUT_FILE` | *(required)* | DSM2 `.inp` file containing the updated table |
| `TIDEFILE_PATH` | `/hydro/input/channel` | HDF5 dataset path to overwrite |
| `TABLE_NAME` | `CHANNEL` | Table name to read from the `.inp` file |

```bash
pydsm update-hydro-tidefile-with-inp historical.h5 modified_channels.inp
```

---

#### `create-gtm-restart`

Write a GTM/Qual restart file from an HDF5 tidefile at the nearest stored time step to a target time.

```
pydsm create-gtm-restart TIDEFILE TARGET_TIME OUTFILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `TIDEFILE` | *(required)* | GTM/Qual HDF5 tidefile |
| `TARGET_TIME` | *(required)* | Desired time, e.g. `"05FEB2020 0300"`. Nearest model output time is used. |
| `OUTFILE` | *(required)* | Destination restart file path |
| `-c / --constituent` | `ec` | Constituent to export |

```bash
pydsm create-gtm-restart qual_output.h5 "01OCT2015 0000" restart_oct2015.inp
```

---

#### `calc-volumes`

Calculate DSM2 channel and/or reservoir volumes from a Hydro HDF5 tidefile.

```
pydsm calc-volumes HYDROFILE [OPTIONS]
```

| Argument / Option | Default | Description |
|---|---|---|
| `HYDROFILE` | *(required)* | Hydro HDF5 tidefile |
| `--timewindow` | full run period | Time window, e.g. `"01JAN2014 - 01JAN2015"` |
| `--channel` | all | Channel number to include (repeatable) |
| `--channel-file` | — | Text file with one channel number per line |
| `--reservoir` | all | Reservoir name to include (repeatable) |
| `--reservoir-file` | — | Text file with one reservoir name per line |
| `--unit` | `acre-feet` | Volume unit. Choices: `cubic-feet`, `acre-feet`, `maf` |
| `--no-channels` | off | Skip channel volume calculation |
| `--no-reservoirs` | off | Skip reservoir volume calculation |
| `-o / --output` | `volumes.csv` | Output CSV file path |

```bash
pydsm calc-volumes historical.h5 --timewindow "01JAN2014 - 01JAN2015" -o volumes_2014.csv
```

---
