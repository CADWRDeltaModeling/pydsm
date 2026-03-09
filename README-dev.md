# pydsm — Developer Guide

This document covers environment setup for both **users** (who just want to run pydsm) and **developers** (who want to run tests, modify the code, or build the docs).

---

## Prerequisites

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
- The `cadwr-dms` conda channel is required for `pyhecdss` and `vtools3`

---

## User Installation

If you just want to use pydsm in your own project:

```bash
conda env create -f environment.yml
conda activate pydsm
```

This installs the core runtime plus visualization extras (`geopandas`, `shapely`)
as defined in [`environment.yml`](environment.yml), which mirrors
`[project.dependencies]` + `[project.optional-dependencies.viz]` from [`pyproject.toml`](pyproject.toml).

---

## Developer Installation

The developer environment adds testing tools (`pytest`, `pyarrow`, `black`, `flake8`)
on top of the core runtime and viz extras.  It does **not** include documentation tools.

```bash
conda env create -f environment_dev.yml
conda activate pydsm-dev
```

`environment_dev.yml` runs `pip install -e .` automatically, so the package is
immediately importable in editable mode — no separate install step needed.

If you prefer to manage the environment yourself you can also do:

```bash
pip install -e ".[dev]"   # installs test + docs extras declared in pyproject.toml
```

---

## Running the Test Suite

```bash
pytest tests/
```

### Regression fixtures

Several tests compare DataFrame output against stored **Parquet snapshots** in
`tests/data/`.  These are checked into the repository and should be stable
across library upgrades because Parquet is a format-stable columnar format
(unlike pickle, which encodes Python/numpy internals).

The fixture logic lives in [`tests/conftest.py`](tests/conftest.py).  Tests
accept the `assert_frame_fixture` fixture as a parameter; it loads the
corresponding `.parquet` file and compares with `pd.testing.assert_frame_equal`.

#### When to regenerate fixtures

Regenerate fixtures when:

- A **deliberate change** to a reader method changes the expected output, **or**
- A **major library upgrade** (pandas, numpy) changes dtype or value representation

```bash
pytest --regen-fixtures tests/
```

This overwrites every `.parquet` file in `tests/data/` with the current output.
After regenerating, review the diff carefully (`git diff tests/data/`) and commit
the updated fixtures alongside the code change that caused them.

#### Adding a new fixture

1. Add the `assert_frame_fixture` parameter to your test method.
2. Call `assert_frame_fixture(df, "my_fixture_name")`.
3. Run `pytest --regen-fixtures tests/test_yourfile.py::your_test` once to
   create `tests/data/my_fixture_name.parquet`.
4. Commit `tests/data/my_fixture_name.parquet` together with the test.

---

## Building the Documentation

A separate minimal environment is provided for documentation builds:

```bash
conda env create -f environment-docs.yml
conda activate pydsm-docs
```

Build the HTML docs:

```bash
cd docsrc
make html        # output goes to docsrc/_build/html/
```

Live-reload during writing:

```bash
sphinx-autobuild docsrc docsrc/_build/html
```

---

## Optional extras summary

| Extra | Install | What it adds |
|---|---|---|
| `viz` | `pip install "pydsm[viz]"` | `geopandas`, `shapely` (for `pydsm.viz`) |
| `test` | `pip install "pydsm[test]"` | `pytest`, `pytest-cov`, `pyarrow` |
| `docs` | `pip install "pydsm[docs]"` | Sphinx and related tools |
| `dev` | `pip install "pydsm[dev]"` | All of `test` + `docs` + `black`, `flake8`, `twine` |

---

## Environment file overview

| File | Purpose |
|---|---|
| [`environment.yml`](environment.yml) | User runtime — core + viz |
| [`environment_dev.yml`](environment_dev.yml) | Developer — core + viz + test tools |
| [`environment-docs.yml`](environment-docs.yml) | Full — core + viz + test + docs (Python 3.11 pinned) |
