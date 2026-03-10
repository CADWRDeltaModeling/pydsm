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

The `pydsm` command-line tool provides the following subcommands:

```
pydsm --help
```

### DSS file utilities

| Command | Description |
|---------|-------------|
| [`pydsm extract-dss`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#extract-dss) `<dssfile>` | Extract time series from a DSS file; optionally filter by C-part, apply Godin filter, and resample to daily/monthly values. Output can be pickle (`.gz`/`.zip`/`.bz2`), HDF5 (`.h5`), or DSS. |
| [`pydsm compare-dss`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#compare-dss) `<dssfile1> <dssfile2>` | Compare two DSS files on matching B/C-part pathnames and write goodness-of-fit metrics (RMSE, Nash-Sutcliffe, percent bias, etc.) to a CSV. |
| [`pydsm copy-all-dss`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#copy-all-dss) `<from_file> <to_file>` | Copy all paths from one DSS file to another. |
| [`pydsm csv-to-dss`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#csv-to-dss) `<csv_file> <dss_file>` | Convert a CSV time series file to a DSS file with configurable path parts, units, period type, resampling, and a numeric multiplier. |
| [`pydsm extend-dss-ts`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#extend-dss-ts) `<dss_filename> <dss_ext_filename>` | Extend time series in a DSS file by a number of days (default 366) by appending a shifted copy at the end. |
| [`pydsm repeating`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#repeating) `create <datafile>` | Create a repeating annual time series from a CSV template year and write it to a DSS file. |
| [`pydsm repeating`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#repeating) `extend <datafile>` | Extend an existing repeating time series in a DSS file forward to a given end year. |

### DSM2 input / echo files

| Command | Description |
|---------|-------------|
| [`pydsm pretty-print-input`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#pretty-print-input) `<input_file> [output_file]` | Reformat (pretty-print) a DSM2 `.inp` echo file. Defaults to `<basename>.pretty.inp`. |
| [`pydsm create-dsm2-input-for-cd`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#create-dsm2-input-for-cd) `<dss_filename> <dsm2_input_filename> <file_field_string>` | Generate a DSM2 `.inp` boundary file for consumptive demand from a DSS file. |
| [`pydsm chan-orient`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#chan-orient) `<channel_line_geojson_file> <hydro_echo_file>` | Generate a channel orientation file (angles) from a GeoJSON channel geometry and a Hydro echo file. |

### HDF5 tidefile utilities

| Command | Description |
|---------|-------------|
| [`pydsm slice-hydro`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#slice-hydro) `<infile> <outfile> <stime> <etime>` | Slice a Hydro HDF5 tidefile to a time window (e.g. `1990-01-10` to `1990-03-31`) and write a new tidefile. |
| [`pydsm update-hydro-tidefile-with-inp`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#update-hydro-tidefile-with-inp) `<hydro_tidefile> <input_file>` | Patch an HDF5 Hydro tidefile's input table from a `.inp` file. |
| [`pydsm create-gtm-restart`](https://cadwrdeltamodeling.github.io/pydsm/pydsm.cli.html#create-gtm-restart) `<tidefile> <target_time> <outfile>` | Write a GTM/Qual restart file from an HDF5 tidefile at the nearest stored time step to `target_time` (e.g. `05FEB2020 0300`). |

---
