"""
Tests for pydsm.analysis.dsm2study:
  - DSM2TimeSeriesReference class
  - _load_dss_ts_table helper
  - DSM2Study (echo-file parsing; HydroH5 is mocked so no HDF5 file is required)

Test data: tests/data/hydro_echo_hist_fc_mss.inp
  A real hydro echo file from the DWR historical DSM2 study (hist_fc_mss,
  run_start_date 01JAN2020, run_end_date 01JAN2024).
"""
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pydsm.analysis.dsm2study import (
    DSM2Study,
    DSM2TimeSeriesReference,
    _load_dss_ts_table,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ECHO_FILE = os.path.join(DATA_DIR, "hydro_echo_hist_fc_mss.inp")


# ---------------------------------------------------------------------------
# Shared mock for HydroH5 (keeps tests independent of the 12 GB HDF5 file)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def mock_source_flow_df():
    """Minimal source_flow DataFrame that satisfies DSM2Study.__init__."""
    return pd.DataFrame(
        {
            "name": ["sac"],
            "file": ["../../timeseries/hist.dss"],
            "path": ["/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/"],
            "sign": [1],
        }
    )


@pytest.fixture(scope="module")
def dsm2_study(mock_source_flow_df):
    """DSM2Study loaded from the test echo file with HydroH5 mocked out."""
    with patch("pydsm.analysis.dsm2study.hydroh5.HydroH5") as MockHydro:
        mock_h5 = MagicMock()
        mock_h5.get_input_table.return_value = mock_source_flow_df
        MockHydro.return_value = mock_h5
        study = DSM2Study(ECHO_FILE)
    return study


# ===========================================================================
# DSM2TimeSeriesReference
# ===========================================================================
class TestDSM2TimeSeriesReference:
    def test_repr_dss(self):
        ref = DSM2TimeSeriesReference(
            name="sac",
            file="/path/to/hist.dss",
            path="/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/",
            sign=1.0,
            fillin="last",
        )
        r = repr(ref)
        assert "DSM2TimeSeriesReference" in r
        assert "sac" in r
        assert "hist.dss" in r

    def test_repr_constant(self):
        ref = DSM2TimeSeriesReference(name="op", file="constant", path="1.0")
        r = repr(ref)
        assert "constant" in r

    def test_is_constant_true_lowercase(self):
        ref = DSM2TimeSeriesReference(name="x", file="constant", path="42.0")
        assert ref.is_constant is True

    def test_is_constant_true_mixed_case(self):
        ref = DSM2TimeSeriesReference(name="x", file="CONSTANT", path="0.0")
        assert ref.is_constant is True

    def test_is_constant_false(self):
        ref = DSM2TimeSeriesReference(
            name="sac", file="/data/hist.dss", path="/A/B/FLOW//15MIN/F/"
        )
        assert ref.is_constant is False

    def test_get_data_constant_default_sign(self):
        ref = DSM2TimeSeriesReference(name="c", file="constant", path="3.5")
        assert ref.get_data() == pytest.approx(3.5)

    def test_get_data_constant_with_sign(self):
        ref = DSM2TimeSeriesReference(
            name="c", file="constant", path="2.0", sign=-1.0
        )
        assert ref.get_data() == pytest.approx(-2.0)

    def test_get_data_dss_calls_pyhecdss(self):
        """get_data() should call pyhecdss.get_ts with the file and path."""
        fake_ts = pd.Series(
            [10.0, 20.0],
            index=pd.date_range("2020-01-01", periods=2, freq="15min"),
        )
        ref = DSM2TimeSeriesReference(
            name="sac",
            file="/data/hist.dss",
            path="/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/",
            sign=2.0,
        )
        with patch("pydsm.analysis.dsm2study.pyhecdss") as mock_dss:
            mock_dss.get_ts.return_value = iter([(fake_ts, None, None)])
            result = ref.get_data()

        mock_dss.get_ts.assert_called_once_with(ref.file, ref.path)
        pd.testing.assert_series_equal(result, fake_ts * 2.0)

    def test_get_data_dss_timewindow_slices(self):
        """When timewindow is given the returned series is sliced.

        DSM2 timewindow format is ``"01JAN2020 0000 - 01JAN2020 0030"``
        (military date), which uses no dashes inside the date tokens so the
        ``split("-", 1)`` separator in get_data() works unambiguously.
        """
        idx = pd.date_range("2020-01-01", periods=4, freq="15min")
        fake_ts = pd.Series([1.0, 2.0, 3.0, 4.0], index=idx)
        ref = DSM2TimeSeriesReference(
            name="sac", file="/data/hist.dss", path="/A/B/C//15MIN/F/"
        )
        with patch("pydsm.analysis.dsm2study.pyhecdss") as mock_dss:
            mock_dss.get_ts.return_value = iter([(fake_ts, None, None)])
            # Only request the first two 15-min steps
            result = ref.get_data(
                timewindow="01JAN2020 0000 - 01JAN2020 0015"
            )

        assert len(result) <= len(fake_ts)
        assert result.index[0] >= pd.Timestamp("2020-01-01 00:00")


# ===========================================================================
# _load_dss_ts_table
# ===========================================================================
class TestLoadDssTable:
    def _make_bf_table(self):
        """Minimal BOUNDARY_FLOW table (3 rows with SIGN)."""
        return pd.DataFrame(
            {
                "NAME": ["sac", "north_bay", "vernalis"],
                "NODE": [330, 273, 17],
                "SIGN": [1, -1, 1],
                "FILLIN": ["last", "last", "last"],
                "FILE": [
                    "../../timeseries/hist.dss",
                    "../../timeseries/hist.dss",
                    "../../timeseries/hist.dss",
                ],
                "PATH": [
                    "/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/",
                    "/FILL+CHAN/SLBAR002/FLOW-EXPORT//15MIN/DWR-DMS-202312/",
                    "/FILL+CHAN/RSAN112/FLOW//15MIN/DWR-DMS-202312/",
                ],
            }
        )

    def test_returns_dict_keyed_by_name(self):
        result = _load_dss_ts_table(self._make_bf_table(), "NAME", ECHO_FILE)
        assert set(result.keys()) == {"sac", "north_bay", "vernalis"}

    def test_values_are_dsm2_ts_reference(self):
        result = _load_dss_ts_table(self._make_bf_table(), "NAME", ECHO_FILE)
        for v in result.values():
            assert isinstance(v, DSM2TimeSeriesReference)

    def test_sign_preserved(self):
        result = _load_dss_ts_table(self._make_bf_table(), "NAME", ECHO_FILE)
        assert result["sac"].sign == pytest.approx(1.0)
        assert result["north_bay"].sign == pytest.approx(-1.0)

    def test_path_preserved(self):
        result = _load_dss_ts_table(self._make_bf_table(), "NAME", ECHO_FILE)
        assert (
            result["sac"].path == "/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/"
        )

    def test_file_resolved_to_absolute(self):
        result = _load_dss_ts_table(self._make_bf_table(), "NAME", ECHO_FILE)
        assert os.path.isabs(result["sac"].file)
        assert result["sac"].file.endswith("hist.dss")

    def test_no_sign_column_defaults_to_one(self):
        """BOUNDARY_STAGE has no SIGN column; default sign should be 1.0."""
        bs_table = pd.DataFrame(
            {
                "NAME": ["mtz"],
                "NODE": [361],
                "FILLIN": ["linear"],
                "FILE": ["../../timeseries/hist.dss"],
                "PATH": ["/FILL+CHAN/RSAC054/STAGE//15MIN/DWR-DMS-202312/"],
            }
        )
        result = _load_dss_ts_table(bs_table, "NAME", ECHO_FILE)
        assert result["mtz"].sign == pytest.approx(1.0)

    def test_constant_file_not_resolved(self):
        """When FILE is 'constant', the file attribute stays as 'constant'."""
        ots_table = pd.DataFrame(
            {
                "NAME": ["clfct_op"],
                "FILLIN": ["last"],
                "FILE": ["constant"],
                "PATH": ["1.0"],
            }
        )
        result = _load_dss_ts_table(ots_table, "NAME", ECHO_FILE)
        assert result["clfct_op"].is_constant is True
        assert result["clfct_op"].get_data() == pytest.approx(1.0)


# ===========================================================================
# DSM2Study — echo-file parsing (HydroH5 mocked)
# ===========================================================================
class TestDSM2Study:
    # --- Runtime -----------------------------------------------------------

    def test_get_runtime_start(self, dsm2_study):
        start, _ = dsm2_study.get_runtime()
        assert start == datetime(2020, 1, 1, 0, 0)

    def test_get_runtime_end(self, dsm2_study):
        _, end = dsm2_study.get_runtime()
        assert end == datetime(2024, 1, 1, 0, 0)

    # --- BOUNDARY_FLOW -----------------------------------------------------

    def test_boundary_flow_count(self, dsm2_study):
        assert len(dsm2_study.boundary_flow) == 10

    def test_boundary_flow_expected_keys(self, dsm2_study):
        expected = {
            "bc_150", "bc_151", "calaveras", "cosumnes", "moke",
            "north_bay", "sac", "vernalis", "yolo", "yolo_toe",
        }
        assert set(dsm2_study.boundary_flow.keys()) == expected

    def test_boundary_flow_sac_sign(self, dsm2_study):
        assert dsm2_study.boundary_flow["sac"].sign == pytest.approx(1.0)

    def test_boundary_flow_sac_path(self, dsm2_study):
        assert (
            dsm2_study.boundary_flow["sac"].path
            == "/FILL+CHAN/RSAC155/FLOW//15MIN/DWR-DMS-202312/"
        )

    def test_boundary_flow_sac_fillin(self, dsm2_study):
        assert dsm2_study.boundary_flow["sac"].fillin == "last"

    def test_boundary_flow_north_bay_negative_sign(self, dsm2_study):
        """north_bay is an export, so SIGN should be -1."""
        assert dsm2_study.boundary_flow["north_bay"].sign == pytest.approx(-1.0)

    def test_boundary_flow_file_is_absolute(self, dsm2_study):
        assert os.path.isabs(dsm2_study.boundary_flow["sac"].file)

    def test_boundary_flow_are_dsm2_ts_references(self, dsm2_study):
        for v in dsm2_study.boundary_flow.values():
            assert isinstance(v, DSM2TimeSeriesReference)

    # --- BOUNDARY_STAGE ----------------------------------------------------

    def test_boundary_stage_count(self, dsm2_study):
        assert len(dsm2_study.boundary_stage) == 1

    def test_boundary_stage_mtz_key(self, dsm2_study):
        assert "mtz" in dsm2_study.boundary_stage

    def test_boundary_stage_mtz_path(self, dsm2_study):
        assert (
            dsm2_study.boundary_stage["mtz"].path
            == "/FILL+CHAN/RSAC054/STAGE//15MIN/DWR-DMS-202312/"
        )

    def test_boundary_stage_default_sign(self, dsm2_study):
        """BOUNDARY_STAGE has no SIGN column; default should be 1.0."""
        assert dsm2_study.boundary_stage["mtz"].sign == pytest.approx(1.0)

    # --- SOURCE_FLOW -------------------------------------------------------

    def test_source_flow_ts_count(self, dsm2_study):
        # hist_fc_mss SOURCE_FLOW has 1020 rows (primarily DICU entries)
        assert len(dsm2_study.source_flow_ts) == 1020

    def test_source_flow_ts_exports_negative(self, dsm2_study):
        """Known exports/diversions must carry SIGN = -1."""
        for name in ("ccc", "cvp", "ccw"):
            assert dsm2_study.source_flow_ts[name].sign == pytest.approx(-1.0), (
                f"{name} should be a diversion (sign=-1)"
            )

    # --- SOURCE_FLOW_RESERVOIR ---------------------------------------------

    def test_source_flow_reservoir_count(self, dsm2_study):
        assert len(dsm2_study.source_flow_reservoir) == 4

    def test_source_flow_reservoir_swp_sign(self, dsm2_study):
        """SWP is a major export from clifton_court; SIGN should be -1."""
        assert dsm2_study.source_flow_reservoir["swp"].sign == pytest.approx(-1.0)

    def test_source_flow_reservoir_drain_positive_sign(self, dsm2_study):
        """DICU drain flows return water to the reservoir; SIGN should be +1."""
        assert (
            dsm2_study.source_flow_reservoir["dicu_drain_BBID"].sign
            == pytest.approx(1.0)
        )

    # --- INPUT_GATE / INPUT_TRANSFER_FLOW (empty in this study) ------------

    def test_input_gate_empty(self, dsm2_study):
        assert dsm2_study.input_gate == {}

    def test_input_transfer_flow_empty(self, dsm2_study):
        assert dsm2_study.input_transfer_flow == {}

    # --- OPRULE_TIME_SERIES ------------------------------------------------

    def test_oprule_time_series_count(self, dsm2_study):
        assert len(dsm2_study.oprule_time_series) == 33

    def test_oprule_time_series_constant_entry(self, dsm2_study):
        """clfct_op uses FILE=constant with PATH=1.0."""
        clfct = dsm2_study.oprule_time_series["clfct_op"]
        assert clfct.is_constant is True
        assert clfct.get_data() == pytest.approx(1.0)

    def test_oprule_time_series_non_constant(self, dsm2_study):
        """clfct_height references a real DSS file."""
        clfct_h = dsm2_study.oprule_time_series["clfct_height"]
        assert clfct_h.is_constant is False
        assert "gates.dss" in clfct_h.file

    def test_oprule_time_series_file_is_absolute(self, dsm2_study):
        """Resolved FILE paths for non-constant entries must be absolute."""
        for name, ref in dsm2_study.oprule_time_series.items():
            if not ref.is_constant:
                assert os.path.isabs(ref.file), (
                    f"oprule_time_series[{name!r}].file should be absolute"
                )

    # --- get_output_channels -----------------------------------------------

    def test_get_output_channels_returns_dataframe(self, dsm2_study):
        oc = dsm2_study.get_output_channels()
        assert isinstance(oc, pd.DataFrame)
        assert not oc.empty

    def test_get_output_channels_has_required_columns(self, dsm2_study):
        oc = dsm2_study.get_output_channels()
        for col in ("NAME", "CHAN_NO", "VARIABLE", "INTERVAL"):
            assert col in oc.columns, f"Missing column: {col}"
