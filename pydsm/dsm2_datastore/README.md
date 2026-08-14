# dsm2_datastore CLI scripts

Command-line tools that update the DSS timeseries files used as DSM2 boundary/gate
inputs (`hist.dss`, `gates.dss`, etc.) from processed data-store
sources (`dms_datastore` CSV/repo timeseries or SCHISM `.th` files). Each script
reads a YAML config file describing one or more "sites", pulls the processed data
for the configured time window, and overwrites/extends the matching record in the
target DSS file in place.

All scripts are registered as console entry points in [pyproject.toml](../../pyproject.toml),
so after installing the package (`pip install -e .`) they are available directly on
the command line — no need to call `python path/to/script.py`.

## Available commands

| Command | Script | Config (default) | Purpose |
|---|---|---|---|
| `update_dsm2_flow_dss` | [update_dsm2_flow_dss.py](update_dsm2_flow_dss.py) | [flow_config.yaml](flow_config.yaml) | Update DSM2 flow boundary DSS records from the `dms_datastore` processed repo |
| `update_mrz_stage` | [update_mrz_stage.py](update_mrz_stage.py) | [mrz_config.yaml](mrz_config.yaml) | Update Martinez (MRZ) stage boundary, applying the Antioch/Martinez setup correction ([mrz_anh_setup_correction.csv](mrz_anh_setup_correction.csv)) |
| `update_ccf` | [update_ccf.py](update_ccf.py) | [gate_config.yaml](gate_config.yaml) | Update Clifton Court Forebay (CCF) gate elevation and duplicate-count records |
| `update_dcc` | [update_dcc.py](update_dcc.py) | [gate_config.yaml](gate_config.yaml) | Update the Delta Cross Channel (DCC) gate position record |
| `update_smscg` | [update_smscg.py](update_smscg.py) | [gate_config.yaml](gate_config.yaml) | Update Suisun Marsh Salinity Control Gates (SMSCG): flashboards, boatlock, and radial gate records |
| `update_temporary_barrier` | [update_temporary_barrier.py](update_temporary_barrier.py) | [gate_config.yaml](gate_config.yaml) | Update temporary rock barrier sites (GLC, Middle River, Old River at Tracy) from SCHISM `.th` files |

## Usage

```bash
# Update every site listed in the default config
update_dsm2_flow_dss

# Update a single site by name (site names are the top-level keys in the config)
update_dsm2_flow_dss vernalis

# Point at a different config file
update_dsm2_flow_dss --config path/to/flow_config.yaml

# Same pattern applies to update_mrz_stage / update_ccf / update_dcc
update_ccf --config path/to/gate_config.yaml
update_dcc --config path/to/gate_config.yaml
update_mrz_stage --config path/to/mrz_config.yaml

# update_smscg and update_temporary_barrier always process every site
# defined in the config (no SITE argument)
update_smscg --config path/to/gate_config.yaml
update_temporary_barrier --config path/to/gate_config.yaml
```

Run any command with `--help` to see its options, e.g. `update_dsm2_flow_dss --help`.

> `update_ccf` and `update_dcc` accept a `SITE` argument on the command line but
> currently ignore it and always process their fixed, hard-coded list of sites
> (`ccf_gate_ele`/`ccf_ndup` and `dcc` respectively). `update_smscg` and
> `update_temporary_barrier` don't take a `SITE` argument at all — they process
> every site defined in their config.

## Config file format

Each YAML config defines global anchors (`start_time`, `end_time`, the target DSS
`outfile`) and one entry per site with the fields that script needs, e.g.:

```yaml
start_time: &start_time 2020-01-01 00:00:00
end_time: &end_time 2026-01-01 00:00:00
flow_bc_dss: &flow_bc_dss Z:\dsm2_studies\timeseries\hist_forQAQC.dss

vernalis:
  start: *start_time
  end: *end_time
  station_id: vns
  outfile: *flow_bc_dss
  dss_path: //RSAN112/FLOW//// # must uniquely identify the record in the DSS file
```

- `dss_path` must uniquely match exactly one existing record in `outfile` — the
  script reads that record first to determine units, period type, and the
  overlap/extension window before writing back.
- Data before the config's `start`/`end` window in the existing DSS record is
  left untouched; only the overlap and any extension beyond the existing record's
  end are overwritten or appended.
- See [gate_config.yaml](gate_config.yaml) for the additional fields each gate
  site needs (`processed_file`, `column_name`, and one or more `dss_path_*` keys).

## Dependencies

These scripts additionally require `dms_datastore` (for `read_ts_repo`, used by
`update_dsm2_flow_dss` and `update_mrz_stage`) and `schimpy` (for `read_th`, used
by `update_temporary_barrier`). Both are optional dependencies of `pydsm` and must
be installed separately if not already present in the environment.
