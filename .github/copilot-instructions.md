# pydsm — Copilot Instructions

**pydsm** is a Python library for reading, writing, and post-processing inputs and outputs of **DSM2** (Delta Simulation Model II), a 1D hydrodynamic/water-quality model of the Sacramento–San Joaquin Delta operated by California DWR.

DSM2 documentation: https://cadwrdeltamodeling.github.io/dsm2/
pydsm documentation: https://cadwrdeltamodeling.github.io/pydsm

---

## DSM2 Domain Context

### Four modules

| Module | Role |
|--------|------|
| HYDRO | 1D hydrodynamics: flows, stages, velocities, water surface elevations |
| QUAL | Water quality (conservative + non-conservative constituents) — Lagrangian |
| GTM | Water quality, sediment, mercury — Eulerian–Lagrangian; newer than QUAL |
| PTM | Pseudo-3D particle tracking (fish eggs, larvae, tracer particles) |

HYDRO must run first; its tidefile (HDF5) feeds QUAL, GTM, and PTM.

### Study types
- **Historical** (1990–present): calibration/validation against field data
- **Forecasting** (real-time, 1–10 weeks): uses recent data + astronomical tide forecast
- **Planning**: driven by 1921–2021 CalSim statewide operations sequence; uses 16-year hydrology blocks

### Grid terminology
- **Channel** (`CHAN_NO`): numbered 1-D reach with upstream/downstream node numbers (`UPNODE`, `DOWNNODE`), `LENGTH` (ft), `MANNING`, `DISPERSION`
- **Node**: junction connecting channels and/or reservoirs
- **Reservoir**: off-channel storage (e.g., Bethel Island, Franks Tract)
- **Gate / Transfer**: flow control structures; transfers move flow between nodes

### Units — US customary throughout
- Flow: CFS (cubic feet per second); bulk volume: TAF (thousand acre-feet)
- Length/distance: feet; elevation/stage: feet (NAVD88 or NGVD29)
- Salinity expressed as EC (electrical conductivity, µS/cm) or X2 (km from Golden Gate)

### Key station name conventions
DSS B-parts and `BOUNDARY_FLOW` names use DWR/CDEC station codes  
(e.g., `RSAC155` = Sacramento at Freeport, `RSAN112` = San Joaquin at Vernalis, `RSAC075` = Sacramento at Martinez).  
Station names in `OUTPUT_CHANNEL.NAME` should be geographic, not model-construction names (e.g., `"vernalis"` not `"ch101"`).

---

## DSM2 Input File Format (`.inp` / echo files)

Text, table-based. Sections delimited by `<TABLE_NAME> … END`. Columns are whitespace-separated. Use `${VARNAME}` for ENVVAR substitution.

```
ENVVARS
NAME        VALUE
START_DATE  01OCT1990
END_DATE    30SEP2021
END

SCALAR
NAME             VALUE
run_start_date   ${START_DATE}
run_end_date     ${END_DATE}
END

CHANNEL
CHAN_NO  LENGTH   MANNING  DISPERSION  UPNODE  DOWNNODE
1        19500    0.035    360.0       1       2
END

BOUNDARY_FLOW
NAME      NODE  SIGN  FILLIN  FILE           PATH
sac        330     1  last    ${BNDRYINPUT}  /FILL+CHAN/RSAC155/FLOW//1DAY/${VER}/
END

OUTPUT_CHANNEL
NAME       CHAN_NO  DISTANCE  VARIABLE  INTERVAL  PERIOD_OP  FILE
vernalis       17    0.0      flow      15min     INST       ${OUTPUTDSS}
END
```

**Critical gotchas**:
- `RUN_START_DATE` and `RUN_END_DATE` must always be set in SCALAR; defaults intentionally halt the model.
- `SIGN`: `+1` = source (inflow), `-1` = sink (diversion/export). Absolute value of measured exports must be negated.
- `FILLIN` types: `last` (hold last value), `interp` (linear interpolation), `none`.
- `PERIOD_OP`: `INST` or `AVE`. **Never use daily output for tidal data** — the 25-hour tidal period aliases badly with a 24-hour average window.
- DSS output interval must be `MIN`, `HOUR`, or `DAY` — monthly intervals are no longer supported.
- ENVVARS can reference each other if defined before use.

### IO_FILES section

```
IO_FILES
MODEL   TYPE     IO    INTERVAL  FILENAME
hydro   hdf5     out   1hour     ${HYDROHDF5FILE}
hydro   restart  out   1day      ${QUALRSTFILE}
hydro   echo     out   none      ${DSM2MODIFIER}_hydro_echo.inp
output  none     none  none      ${HYDROOUTFILE}
END
```

`TIDEFILE` section in qual/ptm inputs points at the HYDRO-produced HDF5:
```
TIDEFILE
START_DATE  END_DATE  FILENAME
runtime     length    ${HYDROTIDEFILE}
END
```

---

## HDF5 Tidefile Structure

Output tidefiles (`.h5`) are produced by HYDRO and form the primary binary output.

```
/hydro/
  /input/       # channel, reservoir, gate, boundary tables (as HDF5 datasets)
  /geometry/    # channel locations ("upstream"/"downstream"), qext, xsections
  /data/        # time series datasets:
      "channel flow"       # array [time, channel×location]
      "channel area"
      "channel stage"
      "channel avg area"
      "reservoir flow"
      "reservoir height"
      "qext flow"
      "transfer flow"
/qual/
  /input/       # constituent names
  /output/      # concentration time series, channels & reservoirs
/gtm/           # GTM-specific concentration tracking
```

---

## pydsm Package Architecture

```
pydsm/
  input/        # DSM2 .inp echo file read/write
  output/       # HDF5 tidefile readers (HydroH5, QualH5, GtmH5, PTM)
  analysis/     # post-processing, DSS utilities, Delta metrics
  functions/    # time series math (Godin filter, tidal HL, unit conversion)
  viz/          # GIS/geospatial visualization (optional extras)
```

### Key classes and functions

| Symbol | Location | Purpose |
|--------|----------|---------|
| `parse_input(filepath)` | `pydsm.input.parser` | Read DSM2 `.inp` echo file → `dict[str, DataFrame]` |
| `write_input(tables, filepath)` | `pydsm.input.parser` | Write `dict[str, DataFrame]` → `.inp` echo file |
| `pretty_print_input(filepath)` | `pydsm.input.parser` | Reformat `.inp` file in-place |
| `HydroH5(filename)` | `pydsm.output.hydroh5` | Open hydro tidefile; lazy-loads metadata |
| `hydro.get_channels()` | `HydroH5` | DataFrame of channel definitions |
| `hydro.get_data(name, start, end)` | `HydroH5` | Time series DataFrame for a dataset name |
| `hydro.get_start_end_dates()` | `HydroH5` | `(start_date, end_date)` tuple |
| `QualH5(filename)` | `pydsm.output.qualh5` | Open QUAL tidefile |
| `godin(series)` | `pydsm.functions.tsmath` | Apply Godin tidal filter |
| `tidal_hl(series)` | `pydsm.functions.tidalhl` | Detect tidal highs/lows (Numba-JIT) |
| `taf_to_cfs(val)` / `cfs_to_taf(val)` | `pydsm.functions.tafcfs` | Unit conversion |
| `compare_dss(f1, f2)` | `pydsm.analysis.dssutils` | RMSE / Nash-Sutcliffe / bias between two DSS files |
| `calc_ndo(...)` | `pydsm.analysis.dsm2_utilities` | Net Delta Outflow (NDO) |
| `calc_x2(...)` | `pydsm.analysis.dsm2_x2` | X2 salinity intrusion distance |

### DSS path anatomy
```
/A-PART/B-PART/C-PART//E-PART/F-PART/
         ↑location ↑variable   ↑period type ↑study version
```
Common C-parts: `FLOW`, `STAGE`, `EC`, `VELOCITY`, `CALSIM`.  
Common E-parts: `INST-VAL`, `1HOUR`, `15MIN`, `1DAY`.

### dtype stability rule
The parser enforces deterministic column dtypes via `_COLUMN_DTYPE_MAP` (e.g., `CHAN_NO` → `int64`, `MANNING` → `float64`). When adding new column names to input/output tables, add them to this map if they are numeric to prevent int/float flip-flopping across read→write→read cycles.

---

## Build and Test

```bash
# Development environment (includes test + docs + lint tools)
conda env create -f environment_dev.yml
conda activate pydsm
pip install -e .

# Run tests
pytest tests/

# Regenerate parquet test fixtures after changing expected outputs
pytest --regen-fixtures

# Lint
flake8 pydsm tests
```

The `cadwr-dms` conda channel is required for `pyhecdss` and `vtools3`.

---

## Conventions

- **Primary data structure**: `pandas.DataFrame` with `DatetimeIndex` for all time series; `pandas.DataFrame` for static tables from `.inp` files.
- **Time zones**: DSM2 operates in PST (no daylight saving). Do not mix tz-aware and tz-naive timestamps.
- **Coordinate system**: WGS84/GCS for GIS outputs; channel cross-sections use feet, elevations in feet (NAVD88 or NGVD29 depending on study).
- **vtools3 filters**: Godin and cosine Lanczos filters come from `vtools3`; do not re-implement these locally.
- **File references**: Use `${ENVVAR}` syntax in `.inp` files. Configuration files contain the authoritative ENVVAR definitions.
- **Test fixtures**: Tests use parquet snapshots stored in `tests/`. Use `--regen-fixtures` only after intentional changes to parser output.
- **Optional viz extras**: Import `pydsm.viz` only inside optional code paths; it requires `geopandas`/`shapely` which are not in the core install.

---

## Key Reference Links

| Topic | URL |
|-------|-----|
| DSM2 full documentation | https://cadwrdeltamodeling.github.io/dsm2/ |
| DSM2 input reference (all tables) | https://cadwrdeltamodeling.github.io/dsm2/manual/reference/ |
| DSM2 organizing a study | https://cadwrdeltamodeling.github.io/dsm2/reference/Organizing_a_Study/ |
| Tidefile (HYDRO→QUAL/PTM) | https://cadwrdeltamodeling.github.io/dsm2/manual/reference/Tidefile/ |
| HDF5 output FAQ | https://cadwrdeltamodeling.github.io/dsm2/faqs/DSM2_-_How_to_read_hdf5_output_files/ |
| CalSim–DSM2 integration | https://cadwrdeltamodeling.github.io/dsm2/reference/CALSIM_-_DSM2_Integration/ |
| pydsm API docs | https://cadwrdeltamodeling.github.io/pydsm |
