# pydsm

For working with DSM2 input (`.inp`) and output (`.h5` and `.dss`) with Python (pandas).

- Free software: MIT license
- Documentation: https://cadwrdeltamodeling.github.io/pydsm

---

## Features

- Reads DSM2 HDF5 files into pandas DataFrames
  - Reads Hydro tidefile time series data, input tables, and geometry tables
  - Reads Qual tidefile time series data and input tables
- Reads and writes DSM2 echo files into and from pandas DataFrames
- Shows use of graph network libraries to work with the DSM2 grid as a graph of nodes and channels/reservoirs connecting them

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

## Known Issues

`conda install h5py` lacks szip/zlib support
([upstream issue](https://github.com/ContinuumIO/anaconda-issues/issues/11382)).
Workaround:

```bash
conda uninstall h5py
pip install h5py
```

---

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter)
and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.
