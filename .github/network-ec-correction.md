# Network-Aware EC Correction — pydsm Core Algorithm

See full design document in the `dsm2ui` repository:

`dsm2ui/.github/network-ec-correction.md`

## Summary

This package provides the core algorithm:

- `pydsm/analysis/network_correction.py`
  - `NetworkCorrector` (ABC)
  - `NetworkIDWCorrector` — directed graph, IDW weights
  - `NetworkOICorrector` — undirected graph, OI with exponential/channel-direction kernel
  - `exponential_kernel()`, `channel_direction_kernel()` — kernel factories
  - `extract_channel_end_values()` — QualH5 → channel-end DataFrame
  - `snap_stations_to_channel_ends()` — project x/y stations onto network

The animation integration (`CorrectedQualH5ConcentrationReader`, CLI options,
UI correction card) lives in `dsm2ui/animate.py` and `dsm2ui/animate_cli.py`.

## Test location

`pydsm/tests/test_network_correction.py` — 47 tests covering IDW, OI, both
kernels, SPD guarantee, missing-observation handling, and real H5 integration.
