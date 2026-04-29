"""Tests for dssutils.csv_to_dss — CSV header → DSS B-part conversion."""

import textwrap
from pathlib import Path

import pandas as pd
import pytest

import pyhecdss
from pydsm.analysis.dssutils import csv_to_dss

DATA_DIR = Path(__file__).parent / "data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_dss_paths(dss_file):
    """Return the list of DSS pathnames present in *dss_file*."""
    with pyhecdss.DSSFile(str(dss_file)) as f:
        return f.get_pathnames(f.read_catalog())


def _read_dss_ts(dss_file, pathname):
    """Read a single time series from *dss_file* and return a Series."""
    with pyhecdss.DSSFile(str(dss_file)) as f:
        ts, _unit, _period_type = f.read_rts(pathname)
    if isinstance(ts, pd.DataFrame):
        ts = ts.iloc[:, 0]
    if isinstance(ts.index, pd.PeriodIndex):
        ts.index = ts.index.to_timestamp()
    return ts


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def daily_csv(tmp_path):
    """A minimal CSV with three columns and a daily DatetimeIndex."""
    content = textwrap.dedent("""\
        ,flow_sac,flow_sjr,ec_bdl
        2020-01-01,100.0,50.0,200.0
        2020-01-02,110.0,55.0,210.0
        2020-01-03,120.0,60.0,220.0
        2020-01-04,130.0,65.0,230.0
        2020-01-05,140.0,70.0,240.0
    """)
    p = tmp_path / "test_input.csv"
    p.write_text(content)
    return p


@pytest.fixture
def csv_with_nans(tmp_path):
    """CSV where one column has NaN values (should be skipped for that record)."""
    content = textwrap.dedent("""\
        ,flow_a,flow_b
        2020-01-01,100.0,
        2020-01-02,110.0,55.0
        2020-01-03,120.0,60.0
    """)
    p = tmp_path / "test_nans.csv"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCsvToDssColumnHeaders:
    """Column names become B-parts in the DSS path."""

    def test_paths_match_column_names(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        bparts = [p.split("/")[2].lower() for p in paths]
        assert "flow_sac" in bparts
        assert "flow_sjr" in bparts
        assert "ec_bdl" in bparts

    def test_number_of_paths(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        assert len(paths) == 3

    def test_apart_used(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), apart="MYAPART", resample_to="1D")
        paths = _read_dss_paths(dss)
        assert all(p.split("/")[1].upper() == "MYAPART" for p in paths)

    def test_cpart_used(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), cpart="FLOW", resample_to="1D")
        paths = _read_dss_paths(dss)
        assert all(p.split("/")[3].upper() == "FLOW" for p in paths)

    def test_fpart_used(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), fpart="VER1", resample_to="1D")
        paths = _read_dss_paths(dss)
        assert all(p.split("/")[6].upper() == "VER1" for p in paths)


class TestCsvToDssValues:
    """Data values are written correctly."""

    def test_values_match_input(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        sac_path = next(p for p in paths if "/FLOW_SAC/" in p)
        ts = _read_dss_ts(dss, sac_path)
        assert pytest.approx(ts.iloc[0], rel=1e-4) == 100.0
        assert pytest.approx(ts.iloc[-1], rel=1e-4) == 140.0

    def test_multiplier_applied(self, daily_csv, tmp_path):
        dss = tmp_path / "out.dss"
        csv_to_dss(str(daily_csv), str(dss), multiplier=2.0, resample_to="1D")
        paths = _read_dss_paths(dss)
        sac_path = next(p for p in paths if "/FLOW_SAC/" in p)
        ts = _read_dss_ts(dss, sac_path)
        assert pytest.approx(ts.iloc[0], rel=1e-4) == 200.0

    def test_nan_rows_dropped(self, csv_with_nans, tmp_path):
        """NaN values must not be written (dropna() before write_rts)."""
        dss = tmp_path / "out.dss"
        csv_to_dss(str(csv_with_nans), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        flow_b_path = next((p for p in paths if "/FLOW_B/" in p), None)
        assert flow_b_path is not None
        ts = _read_dss_ts(dss, flow_b_path)
        assert ts.isna().sum() == 0

    def test_all_nan_column_skipped(self, tmp_path):
        """A column that is entirely NaN must be silently skipped."""
        content = textwrap.dedent("""\
            ,flow_a,all_nan
            2020-01-01,100.0,
            2020-01-02,110.0,
            2020-01-03,120.0,
        """)
        csv_file = tmp_path / "all_nan.csv"
        csv_file.write_text(content)
        dss = tmp_path / "all_nan.dss"
        csv_to_dss(str(csv_file), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        bparts = [p.split("/")[2].lower() for p in paths]
        assert "flow_a" in bparts
        assert "all_nan" not in bparts


class TestCsvToDssSingleColumn:
    """A single-column CSV writes one path whose B-part equals the column name."""

    def test_single_column(self, tmp_path):
        content = textwrap.dedent("""\
            datetime,my_station
            2021-06-01,500.0
            2021-06-02,510.0
        """)
        csv_file = tmp_path / "single.csv"
        csv_file.write_text(content)
        dss = tmp_path / "single.dss"
        csv_to_dss(str(csv_file), str(dss), resample_to="1D")
        paths = _read_dss_paths(dss)
        assert len(paths) == 1
        assert paths[0].split("/")[2].lower() == "my_station"
