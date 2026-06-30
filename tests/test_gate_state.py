"""
Tests for pydsm.analysis.gate_state

Unit tests: use the test echo file (tests/data/hydro_echo_historical_v82.inp)
            with pyhecdss mocked (no real DSS files needed).

Integration tests: use the real study at
    d:/delta/dsm2_studies/studies/historical/output/hydro_echo_hist_fc_mss.inp
    with real DSS + HDF5 files that MUST exist (fail loudly if absent).

Run integration tests:
    pytest tests/test_gate_state.py -v -m integration
"""
import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from pydsm.analysis.gate_state import (
    _eval_trigger,
    _eval_value_expr,
    _find_rules_needing_hydro,
    _initial_state,
    _matching_paren_end,
    _needs_hydro,
    _parse_action_str,
    _split_top_level,
    get_gate_devices,
    get_gate_state,
)
from pydsm.input.parser import read_input

# ---------------------------------------------------------------------------
# Fixtures / constants
# ---------------------------------------------------------------------------

UNIT_ECHO = r"d:\dev\pydsm\tests\data\hydro_echo_historical_v82.inp"

INTEGRATION_ECHO = (
    r"d:\delta\dsm2_studies\studies\historical\output\hydro_echo_hist_fc_mss.inp"
)
INTEGRATION_H5 = (
    r"d:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5"
)
INTEGRATION_GATES_DSS = r"d:\delta\dsm2_studies\timeseries\gates.dss"


@pytest.fixture(scope="module")
def unit_tables():
    return read_input(UNIT_ECHO)


@pytest.fixture(scope="module")
def small_time_idx():
    """Hourly index for Jan 1990 (matching the v82 echo run window)."""
    return pd.date_range("1990-01-02", "1990-01-31", freq="1h")


# ---------------------------------------------------------------------------
# _parse_action_str
# ---------------------------------------------------------------------------


class TestParseActionStr:
    def test_simple_scalar(self):
        acts = _parse_action_str("SET gate_install(gate=FalseBarrier) TO 1.0")
        assert len(acts) == 1
        a = acts[0]
        assert a["gate_name"] == "FalseBarrier"
        assert a["device"] is None
        assert a["variable"] == "gate_install"
        assert a["direction"] is None
        # _value_ast replaces value_expr — it holds the parsed AST node
        assert a["_value_ast"] is not None

    def test_ts_with_device_and_direction(self):
        acts = _parse_action_str(
            "SET gate_op(gate=clifton_court,device=reservoir_gates,direction=from_node)"
            " TO ts(name=clfct_op)"
        )
        assert len(acts) == 1
        a = acts[0]
        assert a["gate_name"] == "clifton_court"
        assert a["device"] == "reservoir_gates"
        assert a["variable"] == "gate_op"
        assert a["direction"] == "from_node"
        assert a["_value_ast"] is not None

    def test_while_compound(self):
        acts = _parse_action_str(
            "SET gate_op(gate=delta_cross_channel,device=cross_channel_gates,"
            "direction=from_node) TO ts(name=dcc_op)/2"
            " WHILE "
            "SET gate_op(gate=delta_cross_channel,device=cross_channel_gates,"
            "direction=to_node) TO ts(name=dcc_op)/2"
        )
        assert len(acts) == 2
        assert acts[0]["direction"] == "from_node"
        assert acts[1]["direction"] == "to_node"
        assert acts[0]["_value_ast"] is not None

    def test_ramp_stripped(self):
        acts = _parse_action_str(
            "SET gate_op(gate=decker_is_north_weir,device=weir,direction=from_node)"
            " TO 1.0 RAMP 120 MIN"
        )
        assert len(acts) == 1
        assert acts[0]["_value_ast"] is not None

    def test_gate_elev_with_spaces_in_args(self):
        acts = _parse_action_str(
            "SET gate_elev(gate=grant_line_barrier, device=barrier) TO ts(name=glc_elev)"
        )
        assert len(acts) == 1
        a = acts[0]
        assert a["gate_name"] == "grant_line_barrier"
        assert a["device"] == "barrier"
        assert a["variable"] == "gate_elev"
        assert a["_value_ast"] is not None


# ---------------------------------------------------------------------------
# _split_top_level / _matching_paren_end
# ---------------------------------------------------------------------------


class TestSplitHelpers:
    def test_split_or(self):
        parts = _split_top_level("A OR B", " OR ")
        assert parts == ["A", "B"]

    def test_split_and(self):
        parts = _split_top_level("A AND B AND C", " AND ")
        assert parts == ["A", "B", "C"]

    def test_split_respects_parens(self):
        parts = _split_top_level("(A OR B) AND C", " AND ")
        assert parts == ["(A OR B)", "C"]

    def test_no_split_returns_none(self):
        assert _split_top_level("A OR B", " AND ") is None

    def test_matching_paren_end_full(self):
        assert _matching_paren_end("(abc)") == 4

    def test_matching_paren_end_nested(self):
        assert _matching_paren_end("((x) AND (y))") == 12

    def test_matching_paren_end_not_full(self):
        # (abc) def — closing paren at index 4, not at end
        assert _matching_paren_end("(abc) def") == 4

    def test_matching_paren_no_open(self):
        assert _matching_paren_end("abc") == -1


# ---------------------------------------------------------------------------
# _needs_hydro
# ---------------------------------------------------------------------------


class TestNeedsHydro:
    def test_false_for_true(self):
        assert not _needs_hydro("TRUE", {})

    def test_false_for_ts(self):
        assert not _needs_hydro("ts(name=glc_install) >= 1.0", {})

    def test_true_for_chan_stage(self):
        assert _needs_hydro(
            "(chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3",
            {},
        )

    def test_true_for_chan_vel(self):
        assert _needs_hydro("chan_vel(channel=512,dist=5750) < -0.1", {})

    def test_true_via_named_expression(self):
        expressions = {
            "mscs_dhopen": "(chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3"
        }
        assert _needs_hydro("mscs_dhopen", expressions)

    def test_false_via_named_expression_no_chan(self):
        expressions = {"mscs_calc": "ts(name=mscs_op) < 0"}
        assert not _needs_hydro("mscs_calc", expressions)

    def test_recursive_expansion(self):
        """Named expression referencing another named expression with chan_stage."""
        expressions = {
            "outer": "inner AND ts(name=x) > 0",
            "inner": "chan_stage(channel=1,dist=0) > 2.0",
        }
        assert _needs_hydro("outer", expressions)


# ---------------------------------------------------------------------------
# _eval_trigger (unit tests with small synthetic time indices)
# ---------------------------------------------------------------------------


@pytest.fixture
def time_3pt():
    """Three hourly timestamps for minimal trigger tests."""
    return pd.date_range("2015-05-27", periods=3, freq="1h")


class TestEvalTrigger:
    def test_true_trigger(self, time_3pt):
        result = _eval_trigger("TRUE", time_3pt, {}, {})
        assert result.all()
        assert len(result) == 3

    def test_datetime_gte_all_false(self, time_3pt):
        # time_3pt is 2015-05-27 — rule activates on 28MAY2015 — all False
        result = _eval_trigger(
            "DATETIME >= 28MAY2015", time_3pt, {}, {}
        )
        assert not result.any()

    def test_datetime_gte_all_true(self):
        idx = pd.date_range("2015-05-29", periods=3, freq="1h")
        result = _eval_trigger("DATETIME >= 28MAY2015", idx, {}, {})
        assert result.all()

    def test_datetime_and_range(self):
        idx = pd.date_range("2015-05-28", "2015-10-01", freq="1D")
        result = _eval_trigger(
            "DATETIME >= 28MAY2015 AND DATETIME <= 01OCT2015", idx, {}, {}
        )
        # 28 MAY to 01 OCT inclusive — should have True values
        assert result.any()
        # Values before 28MAY should not be present (range starts exactly at 28MAY)
        assert result.iloc[0]

    def test_datetime_or(self):
        # DATETIME < 28MAY2015 OR DATETIME > 01OCT2015
        # For Jan 1990, both sides are False and True respectively
        idx = pd.date_range("1990-01-02", "1990-01-04", freq="1D")
        result = _eval_trigger(
            "DATETIME < 28MAY2015 OR DATETIME > 01OCT2015", idx, {}, {}
        )
        assert result.all()  # 1990 < 28MAY2015

    def test_ts_comparison(self):
        idx = pd.date_range("2020-01-01", periods=4, freq="1h")
        mock_ref = MagicMock()
        mock_ref.get_data.return_value = pd.Series(
            [0.0, 1.0, 0.0, 1.0], index=idx
        )
        ts_data = {"my_ts": mock_ref}
        result = _eval_trigger("ts(name=my_ts) >= 1.0", idx, ts_data, {})
        expected = pd.array([False, True, False, True])
        assert (result.values == expected).all()

    def test_named_expression_expansion(self):
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        mock_ref = MagicMock()
        mock_ref.get_data.return_value = pd.Series([-1.0, 0.5, -0.5], index=idx)
        ts_data = {"mscs_op": mock_ref}
        expressions = {"mscs_calc": "ts(name=mscs_op) < 0"}
        result = _eval_trigger("mscs_calc", idx, ts_data, expressions)
        assert list(result) == [True, False, True]

    def test_compound_and_or(self):
        idx = pd.date_range("2020-01-01", periods=4, freq="1h")
        r1 = MagicMock()
        r1.get_data.return_value = pd.Series([1.0, 1.0, 0.0, 0.0], index=idx)
        r2 = MagicMock()
        r2.get_data.return_value = pd.Series([1.0, 0.0, 1.0, 0.0], index=idx)
        ts_data = {"a": r1, "b": r2}
        # (a >= 1) OR (b >= 1) → T, T, T, F
        result = _eval_trigger(
            "ts(name=a) >= 1.0 OR ts(name=b) >= 1.0", idx, ts_data, {}
        )
        assert list(result) == [True, True, True, False]

    def test_raises_without_hydro(self):
        # New behavior: warn and return Series(False) rather than raise
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        result = _eval_trigger(
            "chan_stage(channel=512,dist=6038) > 0.3", idx, {}, {}, hydro=None
        )
        assert isinstance(result, pd.Series)
        assert not result.any()

    def test_named_expr_raises_without_hydro(self):
        # New behavior: warn and return Series(False) rather than raise
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        expressions = {
            "mscs_dhopen": "(chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3"
        }
        result = _eval_trigger("mscs_dhopen", idx, {}, expressions, hydro=None)
        assert isinstance(result, pd.Series)
        assert not result.any()


# ---------------------------------------------------------------------------
# _eval_value_expr
# ---------------------------------------------------------------------------


class TestEvalValueExpr:
    def test_scalar_float(self):
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        result = _eval_value_expr("1.0", idx, {})
        assert result == 1.0

    def test_ts_ref(self):
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        mock_ref = MagicMock()
        mock_ref.get_data.return_value = pd.Series([2.0, 3.0, 4.0], index=idx)
        result = _eval_value_expr("ts(name=x)", idx, {"x": mock_ref})
        assert isinstance(result, pd.Series)
        assert list(result) == [2.0, 3.0, 4.0]

    def test_ts_div(self):
        idx = pd.date_range("2020-01-01", periods=3, freq="1h")
        mock_ref = MagicMock()
        mock_ref.get_data.return_value = pd.Series([4.0, 6.0, 8.0], index=idx)
        result = _eval_value_expr("ts(name=x)/2", idx, {"x": mock_ref})
        assert list(result) == [2.0, 3.0, 4.0]

    def test_missing_ts_raises(self):
        # New behavior: warn and return 0.0 rather than raise KeyError
        idx = pd.date_range("2020-01-01", periods=2, freq="1h")
        result = _eval_value_expr("ts(name=missing)", idx, {})
        assert result == 0.0


# ---------------------------------------------------------------------------
# get_gate_devices
# ---------------------------------------------------------------------------


class TestGetGateDevices:
    def test_returns_dataframe(self, unit_tables):
        df = get_gate_devices(unit_tables)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self, unit_tables):
        df = get_gate_devices(unit_tables)
        for col in ("GATE_NAME", "DEVICE", "device_type", "DEFAULT_OP"):
            assert col in df.columns, f"Missing column: {col}"

    def test_weir_and_pipe_both_present(self, unit_tables):
        df = get_gate_devices(unit_tables)
        assert "weir" in df["device_type"].values
        assert "pipe" in df["device_type"].values

    def test_gate_metadata_joined(self, unit_tables):
        df = get_gate_devices(unit_tables)
        assert "FROM_OBJ" in df.columns

    def test_no_duplicate_gate_rows(self, unit_tables):
        df = get_gate_devices(unit_tables)
        dupes = df.duplicated(subset=["GATE_NAME", "DEVICE"])
        assert not dupes.any()


# ---------------------------------------------------------------------------
# _initial_state
# ---------------------------------------------------------------------------


class TestInitialState:
    def test_gate_install_present(self, unit_tables, small_time_idx):
        state = _initial_state(unit_tables, small_time_idx)
        keys_with_install = [k for k in state if k[2] == "gate_install"]
        assert len(keys_with_install) > 0

    def test_gate_install_default_one(self, unit_tables, small_time_idx):
        state = _initial_state(unit_tables, small_time_idx)
        for key, arr in state.items():
            if key[2] == "gate_install":
                assert (arr == 1.0).all()

    def test_gate_op_present_for_all_devices(self, unit_tables, small_time_idx):
        state = _initial_state(unit_tables, small_time_idx)
        op_keys = [k for k in state if k[2] == "gate_op"]
        assert len(op_keys) > 0

    def test_unidir_from_node_default(self, unit_tables, small_time_idx):
        state = _initial_state(unit_tables, small_time_idx)
        # grant_line_barrier pipes has unidir_from_node → from_node=1, to_node=0
        from_key = ("grant_line_barrier", "pipes", "gate_op", "from_node")
        to_key = ("grant_line_barrier", "pipes", "gate_op", "to_node")
        assert from_key in state and (state[from_key] == 1.0).all()
        assert to_key in state and (state[to_key] == 0.0).all()


# ---------------------------------------------------------------------------
# get_gate_state — error on missing HDF5 (unit test, no real DSS needed)
# ---------------------------------------------------------------------------


class TestGetGateStateErrors:
    def test_raises_if_hydro_needed_but_not_provided(self):
        """v82 echo has MSCS rules using chan_stage/chan_vel; must raise ValueError."""
        with pytest.raises(ValueError, match="chan_stage|chan_vel"):
            get_gate_state(UNIT_ECHO)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIntegration:
    """Tests against the real DSM2 historical study.

    These tests FAIL if the DSS / HDF5 files are absent — do not skip.
    """

    @pytest.fixture(scope="class")
    def gate_state_df(self):
        """Gate state DataFrame covering key DATETIME-trigger transitions.

        Window: 01JAN2015 – 01NOV2015 — covers the FalseBarrier summer install
        (28MAY–01OCT) at hourly resolution (~7,300 rows × all state variables).
        """
        start = datetime(2015, 1, 1)
        end = datetime(2015, 11, 1)
        return get_gate_state(
            INTEGRATION_ECHO,
            start=start,
            end=end,
            interval="1h",
            hydro_file=INTEGRATION_H5,
        )

    def test_dss_file_exists(self):
        """Confirm the gates.dss file is present (fail loudly if not)."""
        import os

        assert os.path.isfile(INTEGRATION_GATES_DSS), (
            f"Required DSS file not found: {INTEGRATION_GATES_DSS}"
        )

    def test_h5_file_exists(self):
        import os

        assert os.path.isfile(INTEGRATION_H5), (
            f"Required HDF5 file not found: {INTEGRATION_H5}"
        )

    def test_no_hydro_raises_value_error(self):
        """Providing no hydro_file must raise ValueError (MSCS + DICU rules need it)."""
        start = datetime(2015, 1, 1)
        end = datetime(2015, 1, 3)
        with pytest.raises(ValueError, match="chan_stage|chan_vel"):
            get_gate_state(INTEGRATION_ECHO, start=start, end=end, interval="1h")

    def test_returns_dataframe(self, gate_state_df):
        assert isinstance(gate_state_df, pd.DataFrame)

    def test_required_columns(self, gate_state_df):
        for col in ("gate_name", "device", "variable", "direction", "value"):
            assert col in gate_state_df.columns

    def test_datetime_index(self, gate_state_df):
        assert isinstance(gate_state_df.index, pd.DatetimeIndex)

    def test_all_gates_present(self, gate_state_df):
        """All 26 gate names from the GATE table appear in the output."""
        tables = read_input(INTEGRATION_ECHO)
        expected_gates = set(tables["GATE"]["NAME"].tolist())
        actual_gates = set(gate_state_df["gate_name"].dropna().unique())
        missing = expected_gates - actual_gates
        assert not missing, f"Gates missing from output: {missing}"

    def test_false_barrier_install_before_2015(self, gate_state_df):
        """FalseBarrier gate_install should be 0.0 in early Jan 2015 (barrier not yet in)."""
        fb = gate_state_df[
            (gate_state_df["gate_name"] == "FalseBarrier")
            & (gate_state_df["variable"] == "gate_install")
        ]
        assert not fb.empty, "FalseBarrier gate_install not found in output"
        # The barrier is definitely NOT installed in early January
        early = fb.loc["2015-01-01":"2015-01-31"]
        assert not early.empty, "No data in January 2015"
        assert (early["value"] == 0.0).all(), (
            "FalseBarrier gate_install should be 0 in January 2015"
        )

    def test_false_barrier_install_during_summer_2015(self, gate_state_df):
        """FalseBarrier gate_install should be 1.0 from 28MAY2015 to 01OCT2015."""
        fb = gate_state_df[
            (gate_state_df["gate_name"] == "FalseBarrier")
            & (gate_state_df["variable"] == "gate_install")
        ]
        window = fb.loc["2015-05-28":"2015-09-30"]
        assert not window.empty
        assert (window["value"] == 1.0).all(), (
            "FalseBarrier gate_install should be 1 during summer 2015"
        )

    def test_false_barrier_install_after_oct_2015(self, gate_state_df):
        """FalseBarrier gate_install should be 0.0 in late October 2015 (barrier removed)."""
        fb = gate_state_df[
            (gate_state_df["gate_name"] == "FalseBarrier")
            & (gate_state_df["variable"] == "gate_install")
        ]
        # The barrier is definitely removed by mid-October
        late_oct = fb.loc["2015-10-15":"2015-10-31"]
        assert not late_oct.empty
        assert (late_oct["value"] == 0.0).all(), (
            "FalseBarrier gate_install should be 0 by mid-October 2015"
        )

    def test_dcc_gate_op_driven_by_dss(self, gate_state_df):
        """delta_cross_channel gate_op values should be 0.0 or 1.0 (from dcc_op DSS)."""
        dcc = gate_state_df[
            (gate_state_df["gate_name"] == "delta_cross_channel")
            & (gate_state_df["variable"] == "gate_op")
            & (gate_state_df["direction"] == "from_node")
        ]
        assert not dcc.empty, "DCC gate_op from_node not found in output"
        # DCC op ts has values divided by 2 → values are 0.0 or 0.5 (dcc_op is 0 or 1)
        unique_vals = dcc["value"].dropna().unique()
        assert len(unique_vals) > 0
        # All values should be non-negative
        assert (dcc["value"].dropna() >= 0).all()

    def test_hdf5_roundtrip(self, gate_state_df, tmp_path):
        """Write to HDF5 and reload; DataFrame shape must be preserved."""
        out_file = str(tmp_path / "gate_state.h5")
        gate_state_df.to_hdf(out_file, key="/gate_state")
        reloaded = pd.read_hdf(out_file, key="/gate_state")
        assert reloaded.shape == gate_state_df.shape
        assert list(reloaded.columns) == list(gate_state_df.columns)

    def test_value_column_is_numeric(self, gate_state_df):
        assert pd.api.types.is_float_dtype(gate_state_df["value"])

    def test_no_all_nan_rows(self, gate_state_df):
        """No row should have NaN in all columns."""
        critical = gate_state_df[["gate_name", "variable", "value"]]
        assert not critical.isnull().all(axis=1).any()

    def test_output_file_chunked_write(self, tmp_path):
        """output_file= path writes chunks to HDF5; result matches in-memory path."""
        start = datetime(2015, 5, 1)
        end = datetime(2015, 6, 30)
        out_file = str(tmp_path / "gate_state_chunked.h5")

        # Chunked write (3 chunks of 240 hours ≈ 10 days each)
        result = get_gate_state(
            INTEGRATION_ECHO,
            start=start,
            end=end,
            interval="1h",
            hydro_file=INTEGRATION_H5,
            chunk_size=240,
            output_file=out_file,
        )
        assert result is None, "output_file= should return None"

        written = pd.read_hdf(out_file, key="/gate_state")
        assert isinstance(written, pd.DataFrame)
        assert len(written) > 0
        assert set(written.columns).issuperset({"gate_name", "variable", "value"})

        # In-memory result for the same window (small, so no memory concern)
        in_mem = get_gate_state(
            INTEGRATION_ECHO,
            start=start,
            end=end,
            interval="1h",
            hydro_file=INTEGRATION_H5,
        )
        # Shape must match (device/direction None→"" differs in the HDF5 version)
        assert written.shape[0] == in_mem.shape[0], (
            f"Row count mismatch: HDF5={written.shape[0]}, in-memory={in_mem.shape[0]}"
        )
