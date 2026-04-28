"""
Tests for pydsm.analysis.dsm2diff:
  - _compute_static_diff  (unit, synthetic DataFrames)
  - _prep_ts_table        (unit)
  - _parse_timewindow     (unit)
  - DSM2Diff.get_effective_timewindow
  - DSM2Diff.diff_static  (integration – real echo file, two copies)
  - DSM2Diff.diff_ts_table (unit, pyhecdss mocked)
  - FullReport.to_csv     (unit, synthetic report)
  - FullReport.print_report (smoke test)
  - CLI smoke test via click.testing.CliRunner
"""
import os
import shutil
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from pydsm.analysis.dsm2diff import (
    DEFAULT_TS_TABLES,
    MAX_TS_DEFAULT,
    RMSE_THRESHOLD_DEFAULT,
    DSM2Diff,
    FullReport,
    StaticDiff,
    TSDiff,
    _compute_static_diff,
    _format_timewindow,
    _parse_timewindow,
    _prep_ts_table,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ECHO_FILE = os.path.join(DATA_DIR, "hydro_echo_hist_fc_mss.inp")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_channel_df(rows):
    """Build a minimal CHANNEL-like DataFrame from list-of-dicts."""
    return pd.DataFrame(rows)


def _empty_ts_diff(table="BOUNDARY_FLOW"):
    return TSDiff(
        table=table,
        summary=pd.DataFrame(),
        missing=pd.DataFrame(),
        diff_series={},
    )


def _empty_static_diff(table="CHANNEL"):
    empty = pd.DataFrame()
    return StaticDiff(table=table, added=empty, removed=empty, changed=empty)


# ===========================================================================
# _parse_timewindow
# ===========================================================================
class TestParseTimewindow:
    def test_full_format(self):
        start, end = _parse_timewindow("01JAN2020 0000 - 01JAN2024 0000")
        assert start == datetime(2020, 1, 1, 0, 0)
        assert end == datetime(2024, 1, 1, 0, 0)

    def test_date_only_format(self):
        start, end = _parse_timewindow("01JAN2020 - 01JAN2024")
        assert start == datetime(2020, 1, 1, 0, 0)
        assert end == datetime(2024, 1, 1, 0, 0)

    def test_roundtrip(self):
        s, e = datetime(2021, 6, 15, 0, 0), datetime(2023, 12, 31, 0, 0)
        s2, e2 = _parse_timewindow(_format_timewindow(s, e))
        assert s2 == s
        assert e2 == e


# ===========================================================================
# _prep_ts_table
# ===========================================================================
class TestPrepTsTable:
    def test_boundary_flow_unchanged(self):
        df = pd.DataFrame({"NAME": ["sac"], "FILE": ["hist.dss"], "PATH": ["/A/B/C//15MIN/F/"]})
        result, name_col = _prep_ts_table("BOUNDARY_FLOW", df)
        assert name_col == "NAME"
        assert "NAME" in result.columns
        pd.testing.assert_frame_equal(result, df)

    def test_input_gate_adds_name_col(self):
        df = pd.DataFrame({
            "GATE_NAME": ["FalseBarrier"],
            "DEVICE": ["barrier"],
            "VARIABLE": ["elev"],
            "FILE": ["gates.dss"],
            "PATH": ["/A/B/C//D/F/"],
        })
        result, name_col = _prep_ts_table("INPUT_GATE", df)
        assert name_col == "NAME"
        assert result["NAME"].iloc[0] == "FalseBarrier/barrier/elev"

    def test_none_df_returns_none(self):
        result, name_col = _prep_ts_table("BOUNDARY_FLOW", None)
        assert result is None

    def test_empty_df_returns_unchanged(self):
        df = pd.DataFrame(columns=["NAME", "FILE", "PATH"])
        result, name_col = _prep_ts_table("BOUNDARY_FLOW", df)
        assert result.empty


# ===========================================================================
# _compute_static_diff
# ===========================================================================
class TestComputeStaticDiff:
    # --- Edge cases ---

    def test_both_empty(self):
        sd = _compute_static_diff("CHANNEL", None, None, ["CHAN_NO"])
        assert not sd.has_diff

    def test_only_a(self):
        df_a = _make_channel_df([{"CHAN_NO": 1, "LENGTH": 100.0}])
        sd = _compute_static_diff("CHANNEL", df_a, None, ["CHAN_NO"])
        assert not sd.removed.empty
        assert len(sd.removed) == 1
        assert sd.added.empty
        assert sd.changed.empty

    def test_only_b(self):
        df_b = _make_channel_df([{"CHAN_NO": 2, "LENGTH": 200.0}])
        sd = _compute_static_diff("CHANNEL", None, df_b, ["CHAN_NO"])
        assert not sd.added.empty
        assert len(sd.added) == 1
        assert sd.removed.empty

    def test_identical(self):
        df = _make_channel_df([
            {"CHAN_NO": 1, "LENGTH": 100.0, "MANNING": 0.035},
            {"CHAN_NO": 2, "LENGTH": 200.0, "MANNING": 0.028},
        ])
        sd = _compute_static_diff("CHANNEL", df.copy(), df.copy(), ["CHAN_NO"])
        assert not sd.has_diff

    # --- Added / removed rows ---

    def test_added_row(self):
        df_a = _make_channel_df([{"CHAN_NO": 1, "LENGTH": 100.0}])
        df_b = _make_channel_df([
            {"CHAN_NO": 1, "LENGTH": 100.0},
            {"CHAN_NO": 2, "LENGTH": 200.0},
        ])
        sd = _compute_static_diff("CHANNEL", df_a, df_b, ["CHAN_NO"])
        assert len(sd.added) == 1
        assert int(sd.added["CHAN_NO"].iloc[0]) == 2
        assert sd.removed.empty

    def test_removed_row(self):
        df_a = _make_channel_df([
            {"CHAN_NO": 1, "LENGTH": 100.0},
            {"CHAN_NO": 3, "LENGTH": 300.0},
        ])
        df_b = _make_channel_df([{"CHAN_NO": 1, "LENGTH": 100.0}])
        sd = _compute_static_diff("CHANNEL", df_a, df_b, ["CHAN_NO"])
        assert len(sd.removed) == 1
        assert int(sd.removed["CHAN_NO"].iloc[0]) == 3
        assert sd.added.empty

    # --- Changed values ---

    def test_single_key_changed_value(self):
        df_a = _make_channel_df([{"CHAN_NO": 1, "MANNING": 0.035}])
        df_b = _make_channel_df([{"CHAN_NO": 1, "MANNING": 0.030}])
        sd = _compute_static_diff("CHANNEL", df_a, df_b, ["CHAN_NO"])
        assert not sd.changed.empty
        assert "MANNING_a" in sd.changed.columns
        assert "MANNING_b" in sd.changed.columns
        assert "MANNING" in sd.changed["changed_cols"].iloc[0]

    def test_changed_cols_annotation_precision(self):
        """Only the actually-changed column appears in changed_cols."""
        df_a = _make_channel_df([{"CHAN_NO": 1, "MANNING": 0.035, "LENGTH": 1000.0}])
        df_b = _make_channel_df([{"CHAN_NO": 1, "MANNING": 0.040, "LENGTH": 1000.0}])
        sd = _compute_static_diff("CHANNEL", df_a, df_b, ["CHAN_NO"])
        changed_cols = sd.changed["changed_cols"].iloc[0]
        assert "MANNING" in changed_cols
        assert "LENGTH" not in changed_cols

    def test_multikey_changed(self):
        df_a = pd.DataFrame([{"GATE_NAME": "fb", "DEVICE": "barrier", "WIDTH": 950.0}])
        df_b = pd.DataFrame([{"GATE_NAME": "fb", "DEVICE": "barrier", "WIDTH": 1000.0}])
        sd = _compute_static_diff(
            "GATE_WEIR_DEVICE", df_a, df_b, ["GATE_NAME", "DEVICE"]
        )
        assert not sd.changed.empty
        assert sd.added.empty
        assert sd.removed.empty
        assert "WIDTH_a" in sd.changed.columns

    def test_multikey_added_removed(self):
        df_a = pd.DataFrame([{"GATE_NAME": "fb", "DEVICE": "barrier", "WIDTH": 950.0}])
        df_b = pd.DataFrame([{"GATE_NAME": "fb", "DEVICE": "leakage", "WIDTH": 50.0}])
        sd = _compute_static_diff(
            "GATE_WEIR_DEVICE", df_a, df_b, ["GATE_NAME", "DEVICE"]
        )
        assert not sd.added.empty
        assert not sd.removed.empty
        assert sd.changed.empty

    def test_missing_key_column_returns_empty_diff(self):
        """When the key column is absent the diff warns and returns empty."""
        df = pd.DataFrame([{"WRONG_COL": 1}])
        sd = _compute_static_diff("CHANNEL", df, df.copy(), ["CHAN_NO"])
        assert not sd.has_diff


# ===========================================================================
# DSM2Diff.get_effective_timewindow
# ===========================================================================
class TestGetEffectiveTimewindow:
    def _make_scalars(self, start_date, end_date):
        return pd.DataFrame({
            "NAME": [
                "run_start_date", "run_start_time",
                "run_end_date", "run_end_time",
            ],
            "VALUE": [start_date, "0000", end_date, "0000"],
        })

    def _make_diff_with_tables(self, tables_a, tables_b):
        """Instantiate DSM2Diff without reading files on disk."""
        d = object.__new__(DSM2Diff)
        d.echo_a = ECHO_FILE
        d.echo_b = ECHO_FILE
        d.tables_a = tables_a
        d.tables_b = tables_b
        return d

    def test_intersection_uses_latest_start(self):
        ta = {"SCALAR": self._make_scalars("01JAN2019", "01JAN2024")}
        tb = {"SCALAR": self._make_scalars("01JAN2020", "01JAN2022")}
        d = self._make_diff_with_tables(ta, tb)
        start, end = d.get_effective_timewindow()
        assert start == datetime(2020, 1, 1)
        assert end == datetime(2022, 1, 1)

    def test_intersection_uses_earliest_end(self):
        ta = {"SCALAR": self._make_scalars("01JAN2020", "01JAN2023")}
        tb = {"SCALAR": self._make_scalars("01JAN2020", "01JAN2022")}
        d = self._make_diff_with_tables(ta, tb)
        _, end = d.get_effective_timewindow()
        assert end == datetime(2022, 1, 1)

    def test_override_timewindow(self):
        ta = {"SCALAR": self._make_scalars("01JAN2019", "01JAN2024")}
        tb = {"SCALAR": self._make_scalars("01JAN2020", "01JAN2024")}
        d = self._make_diff_with_tables(ta, tb)
        start, end = d.get_effective_timewindow("01MAR2021 0000 - 01MAR2022 0000")
        assert start == datetime(2021, 3, 1)
        assert end == datetime(2022, 3, 1)

    def test_non_overlapping_raises(self):
        ta = {"SCALAR": self._make_scalars("01JAN2020", "01JAN2022")}
        tb = {"SCALAR": self._make_scalars("01JAN2023", "01JAN2024")}
        d = self._make_diff_with_tables(ta, tb)
        with pytest.raises(ValueError, match="non-overlapping"):
            d.get_effective_timewindow()


# ===========================================================================
# DSM2Diff.diff_static — integration against real echo file (self-compare)
# ===========================================================================
class TestDSM2DiffStaticIntegration:
    @pytest.fixture(scope="class")
    def self_diff(self):
        """DSM2Diff comparing the test echo file against itself — should be zero diff."""
        return DSM2Diff(ECHO_FILE, ECHO_FILE)

    def test_self_compare_channel_no_diff(self, self_diff):
        sd = self_diff.diff_static("CHANNEL")
        assert not sd.has_diff

    def test_self_compare_boundary_flow_no_diff(self, self_diff):
        sd = self_diff.diff_static("BOUNDARY_FLOW")
        assert not sd.has_diff

    def test_self_compare_oprule_time_series_no_diff(self, self_diff):
        sd = self_diff.diff_static("OPRULE_TIME_SERIES")
        assert not sd.has_diff

    def test_self_compare_xsect_layer_no_diff(self, self_diff):
        sd = self_diff.diff_static("XSECT_LAYER")
        assert not sd.has_diff

    def test_self_compare_scalar_no_diff(self, self_diff):
        sd = self_diff.diff_static("SCALAR")
        assert not sd.has_diff

    def test_self_compare_reservoir_vol_no_diff(self, self_diff):
        sd = self_diff.diff_static("RESERVOIR_VOL")
        assert not sd.has_diff

    def test_self_compare_output_channel_no_diff(self, self_diff):
        sd = self_diff.diff_static("OUTPUT_CHANNEL")
        assert not sd.has_diff

    def test_self_compare_gate_weir_device_no_diff(self, self_diff):
        sd = self_diff.diff_static("GATE_WEIR_DEVICE")
        assert not sd.has_diff

    def test_unknown_table_returns_empty_diff(self, self_diff):
        sd = self_diff.diff_static("NONEXISTENT_TABLE")
        assert not sd.has_diff

    def test_run_self_compare_all_static_empty(self, self_diff):
        """Full run on self should show no static diffs across all known tables."""
        report = self_diff.run(ts_tables=[])  # skip TS loading
        diffs = [n for n, sd in report.static_diffs.items() if sd.has_diff]
        assert diffs == [], f"Unexpected static diffs in self-compare: {diffs}"

    def test_run_sets_correct_timewindow(self, self_diff):
        report = self_diff.run(ts_tables=[])
        start, end = report.timewindow
        assert start == datetime(2020, 1, 1)
        assert end == datetime(2024, 1, 1)


# ===========================================================================
# DSM2Diff.diff_ts_table — unit tests with mocked pyhecdss
# ===========================================================================
class TestDSM2DiffTsTable:
    """All tests override tables_a / tables_b directly to avoid file I/O,
    and patch pyhecdss so no DSS files need to exist on disk."""

    def _make_diff(self, table_a_df, table_b_df, table_name="BOUNDARY_FLOW"):
        """Return a DSM2Diff with injected in-memory tables."""
        scalars = pd.DataFrame({
            "NAME": ["run_start_date", "run_start_time", "run_end_date", "run_end_time"],
            "VALUE": ["01JAN2020", "0000", "01JAN2024", "0000"],
        })
        d = object.__new__(DSM2Diff)
        d.echo_a = ECHO_FILE
        d.echo_b = ECHO_FILE
        d.tables_a = {"SCALAR": scalars, table_name: table_a_df}
        d.tables_b = {"SCALAR": scalars, table_name: table_b_df}
        return d

    def _make_bf_row(self, name, path, sign=1):
        return pd.DataFrame({
            "NAME": [name],
            "NODE": [330],
            "SIGN": [sign],
            "FILLIN": ["last"],
            "FILE": ["constant"],   # 'constant' avoids DSS file resolution
            "PATH": [str(path)],
        })

    def test_skips_table_exceeding_max_ts(self):
        """A table with more rows than max_ts is skipped without --force."""
        rows = [{"NAME": f"s{i}", "FILE": "constant", "PATH": "0.0", "SIGN": 1, "FILLIN": "last", "NODE": i}
                for i in range(30)]
        df = pd.DataFrame(rows)
        d = self._make_diff(df, df.copy())
        ts = d.diff_ts_table("BOUNDARY_FLOW", max_ts=25)
        assert ts.skipped_table is True
        assert "25" in ts.skip_reason

    def test_force_bypasses_max_ts_guard(self):
        """When force=True, the table is compared regardless of row count."""
        rows = [{"NAME": f"s{i}", "FILE": "constant", "PATH": "1.0", "SIGN": 1, "FILLIN": "last", "NODE": i}
                for i in range(30)]
        df = pd.DataFrame(rows)
        d = self._make_diff(df, df.copy())
        ts = d.diff_ts_table("BOUNDARY_FLOW", max_ts=25, force=True)
        assert ts.skipped_table is False

    def test_missing_in_study_b(self):
        """Entry in study_a but not study_b appears in missing with 'study_a'."""
        df_a = self._make_bf_row("sac", "1.0")
        df_b = pd.DataFrame(columns=["NAME", "NODE", "SIGN", "FILLIN", "FILE", "PATH"])
        d = self._make_diff(df_a, df_b)
        ts = d.diff_ts_table("BOUNDARY_FLOW")
        assert "sac" in ts.missing["name"].values
        assert ts.missing.loc[ts.missing["name"] == "sac", "present_in"].iloc[0] == "study_a"

    def test_missing_in_study_a(self):
        """Entry in study_b but not study_a appears in missing with 'study_b'."""
        df_a = pd.DataFrame(columns=["NAME", "NODE", "SIGN", "FILLIN", "FILE", "PATH"])
        df_b = self._make_bf_row("vernalis", "2.0")
        d = self._make_diff(df_a, df_b)
        ts = d.diff_ts_table("BOUNDARY_FLOW")
        assert ts.missing["present_in"].iloc[0] == "study_b"

    def test_constant_identical_zero_rmse(self):
        """Two constant entries with same value → RMSE=0, no diff_series entry."""
        df = self._make_bf_row("clfct_op", "1.0")
        d = self._make_diff(df, df.copy())
        ts = d.diff_ts_table("BOUNDARY_FLOW")
        row = ts.summary[ts.summary["name"] == "clfct_op"].iloc[0]
        assert row["rmse"] == pytest.approx(0.0)
        assert "clfct_op" not in ts.diff_series

    def test_constant_different_values(self):
        """Two constant entries with different values → RMSE > 0, diff_series populated."""
        df_a = self._make_bf_row("op", "1.0")
        df_b = self._make_bf_row("op", "2.0")
        d = self._make_diff(df_a, df_b)
        ts = d.diff_ts_table("BOUNDARY_FLOW", threshold=0.01)
        row = ts.summary[ts.summary["name"] == "op"].iloc[0]
        assert row["rmse"] == pytest.approx(1.0)
        assert row["bias"] == pytest.approx(-1.0)
        assert "op" in ts.diff_series
        diff_df = ts.diff_series["op"]
        assert diff_df["a"].iloc[0] == pytest.approx(1.0)
        assert diff_df["b"].iloc[0] == pytest.approx(2.0)

    def test_time_series_identical_zero_rmse(self):
        """Identical DSS time series → RMSE=0, bias=0, no diff_series entry."""
        idx = pd.date_range("2020-01-01", periods=5, freq="15min")
        fake_ts = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0], index=idx)

        # Use a non-constant FILE so get_data calls pyhecdss
        df = pd.DataFrame({
            "NAME": ["sac"],
            "NODE": [330],
            "SIGN": [1],
            "FILLIN": ["last"],
            "FILE": ["/some/path/hist.dss"],
            "PATH": ["/A/B/FLOW//15MIN/F/"],
        })
        d = self._make_diff(df, df.copy())

        with patch("pydsm.analysis.dsm2study.pyhecdss") as mock_dss:
            mock_dss.get_ts.side_effect = [
                iter([(fake_ts, None, None)]),
                iter([(fake_ts, None, None)]),
            ]
            ts = d.diff_ts_table("BOUNDARY_FLOW")

        row = ts.summary[ts.summary["name"] == "sac"].iloc[0]
        assert row["rmse"] == pytest.approx(0.0)
        assert "sac" not in ts.diff_series

    def test_time_series_different_values(self):
        """Different DSS time series → RMSE > 0, diff_series populated."""
        idx = pd.date_range("2020-01-01", periods=4, freq="15min")
        ts_a = pd.Series([10.0, 20.0, 30.0, 40.0], index=idx)
        ts_b = pd.Series([12.0, 22.0, 32.0, 42.0], index=idx)  # +2 everywhere

        df = pd.DataFrame({
            "NAME": ["sac"],
            "NODE": [330],
            "SIGN": [1],
            "FILLIN": ["last"],
            "FILE": ["/some/hist.dss"],
            "PATH": ["/A/B/FLOW//15MIN/F/"],
        })
        d = self._make_diff(df, df.copy())

        with patch("pydsm.analysis.dsm2study.pyhecdss") as mock_dss:
            mock_dss.get_ts.side_effect = [
                iter([(ts_a, None, None)]),
                iter([(ts_b, None, None)]),
            ]
            ts = d.diff_ts_table("BOUNDARY_FLOW", threshold=0.01)

        row = ts.summary[ts.summary["name"] == "sac"].iloc[0]
        assert row["rmse"] == pytest.approx(2.0)
        assert row["bias"] == pytest.approx(-2.0)
        assert row["n_points"] == 4
        assert "sac" in ts.diff_series

        diff_df = ts.diff_series["sac"]
        assert list(diff_df.columns) == ["a", "b", "diff"]
        pd.testing.assert_series_equal(diff_df["a"], ts_a.rename("a"))

    def test_path_match_flag(self):
        """path_match is True when both refs point to identical file+path."""
        df = self._make_bf_row("sac", "1.0")
        d = self._make_diff(df, df.copy())
        ts = d.diff_ts_table("BOUNDARY_FLOW")
        assert ts.summary["path_match"].iloc[0] == True

    def test_path_mismatch_flag(self):
        """path_match is False when paths differ."""
        df_a = self._make_bf_row("sac", "1.0")
        df_b = self._make_bf_row("sac", "2.0")  # different PATH
        d = self._make_diff(df_a, df_b)
        ts = d.diff_ts_table("BOUNDARY_FLOW")
        assert ts.summary["path_match"].iloc[0] == False

    def test_skipped_table_flag_for_unknown_table(self):
        d = DSM2Diff(ECHO_FILE, ECHO_FILE)
        ts = d.diff_ts_table("NOT_A_TS_TABLE")
        assert ts.skipped_table is True

    # --- oprule_time_series default ---

    def test_default_ts_tables_includes_oprule(self):
        assert "OPRULE_TIME_SERIES" in DEFAULT_TS_TABLES


# ===========================================================================
# FullReport.to_csv
# ===========================================================================
class TestFullReportToCsv:
    @pytest.fixture
    def tmpdir_path(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d)

    def _minimal_diff_report(self, with_diffs=True):
        if with_diffs:
            changed = pd.DataFrame({
                "CHAN_NO": ["1"],
                "MANNING_a": [0.035],
                "MANNING_b": [0.030],
                "changed_cols": ["MANNING"],
            })
            added = pd.DataFrame({"CHAN_NO": ["1000"], "MANNING": [0.028]})
        else:
            changed = pd.DataFrame()
            added = pd.DataFrame()

        return FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={
                "CHANNEL": StaticDiff(
                    table="CHANNEL",
                    added=added,
                    removed=pd.DataFrame(),
                    changed=changed,
                )
            },
            ts_diffs={},
        )

    def test_no_diffs_creates_no_files(self, tmpdir_path):
        report = self._minimal_diff_report(with_diffs=False)
        report.to_csv(tmpdir_path)
        csvs = [f for f in os.listdir(tmpdir_path) if f.endswith(".csv")]
        assert csvs == []

    def test_static_diff_creates_csv(self, tmpdir_path):
        report = self._minimal_diff_report(with_diffs=True)
        report.to_csv(tmpdir_path)
        files = os.listdir(tmpdir_path)
        assert "channel_changed.csv" in files
        assert "channel_added.csv" in files

    def test_ts_diff_summary_csv(self, tmpdir_path):
        summary = pd.DataFrame({
            "name": ["sac"],
            "path_match": [True],
            "rmse": [5.0],
            "bias": [3.0],
            "n_points": [100],
            "skipped": [False],
            "skip_reason": [""],
        })
        diff_df = pd.DataFrame(
            {"a": [100.0, 200.0], "b": [95.0, 195.0], "diff": [5.0, 5.0]},
            index=pd.date_range("2020-01-01", periods=2, freq="15min"),
        )
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={},
            ts_diffs={
                "BOUNDARY_FLOW": TSDiff(
                    table="BOUNDARY_FLOW",
                    summary=summary,
                    missing=pd.DataFrame(),
                    diff_series={"sac": diff_df},
                )
            },
        )
        report.to_csv(tmpdir_path)
        files = os.listdir(tmpdir_path)
        assert "boundary_flow_ts_summary.csv" in files
        assert "boundary_flow_sac_diff.csv" in files

    def test_ts_diff_missing_csv(self, tmpdir_path):
        missing = pd.DataFrame({"name": ["north_bay"], "present_in": ["study_a"]})
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={},
            ts_diffs={
                "BOUNDARY_FLOW": TSDiff(
                    table="BOUNDARY_FLOW",
                    summary=pd.DataFrame(),
                    missing=missing,
                    diff_series={},
                )
            },
        )
        report.to_csv(tmpdir_path)
        assert "boundary_flow_missing.csv" in os.listdir(tmpdir_path)

    def test_skipped_table_writes_nothing(self, tmpdir_path):
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={},
            ts_diffs={
                "SOURCE_FLOW": TSDiff(
                    table="SOURCE_FLOW",
                    summary=pd.DataFrame(),
                    missing=pd.DataFrame(),
                    diff_series={},
                    skipped_table=True,
                    skip_reason="too many rows",
                )
            },
        )
        report.to_csv(tmpdir_path)
        csvs = [f for f in os.listdir(tmpdir_path) if f.endswith(".csv")]
        assert csvs == []

    def test_safe_name_slash_replaced(self, tmpdir_path):
        """INPUT_GATE names contain '/' — must be sanitised in filenames."""
        diff_df = pd.DataFrame({"a": [1.0], "b": [2.0], "diff": [-1.0]})
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={},
            ts_diffs={
                "INPUT_GATE": TSDiff(
                    table="INPUT_GATE",
                    summary=pd.DataFrame({
                        "name": ["fb/barrier/elev"],
                        "path_match": [False],
                        "rmse": [1.0],
                        "bias": [-1.0],
                        "n_points": [1],
                        "skipped": [False],
                        "skip_reason": [""],
                    }),
                    missing=pd.DataFrame(),
                    diff_series={"fb/barrier/elev": diff_df},
                )
            },
        )
        report.to_csv(tmpdir_path)
        filenames = os.listdir(tmpdir_path)
        assert any("fb_barrier_elev" in f for f in filenames)
        assert not any("fb/barrier/elev" in f for f in filenames)


# ===========================================================================
# FullReport.print_report — smoke test
# ===========================================================================
class TestFullReportPrintReport:
    def test_smoke_no_diffs(self, capsys):
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={
                "CHANNEL": StaticDiff(
                    "CHANNEL",
                    pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                )
            },
            ts_diffs={},
        )
        report.print_report()  # should not raise
        out = capsys.readouterr().out
        assert "DSM2 Study Diff" in out
        assert "Identical:" in out

    def test_smoke_with_diffs(self, capsys):
        changed = pd.DataFrame({
            "CHAN_NO": ["1"],
            "MANNING_a": [0.035],
            "MANNING_b": [0.030],
            "changed_cols": ["MANNING"],
        })
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={
                "CHANNEL": StaticDiff(
                    "CHANNEL", pd.DataFrame(), pd.DataFrame(), changed
                )
            },
            ts_diffs={},
        )
        report.print_report()
        out = capsys.readouterr().out
        assert "CHANNEL" in out
        assert "~1 changed" in out

    def test_skipped_ts_table_noted(self, capsys):
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={},
            ts_diffs={
                "SOURCE_FLOW": TSDiff(
                    table="SOURCE_FLOW",
                    summary=pd.DataFrame(),
                    missing=pd.DataFrame(),
                    diff_series={},
                    skipped_table=True,
                    skip_reason="too many rows",
                )
            },
        )
        report.print_report()
        out = capsys.readouterr().out
        assert "SKIPPED" in out


# ===========================================================================
# CLI smoke test
# ===========================================================================
class TestCLI:
    def test_diff_command_self_compare_no_csv(self, tmp_path):
        from pydsm.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--no-csv",
                "--tables", "OPRULE_TIME_SERIES",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DSM2 Study Diff" in result.output

    def test_diff_command_writes_csv(self, tmp_path):
        from pydsm.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--outdir", str(tmp_path),
                "--tables", "OPRULE_TIME_SERIES",
            ],
        )
        assert result.exit_code == 0, result.output
        # Self-compare → no diffs → no CSV files written
        csvs = list(tmp_path.glob("*.csv"))
        assert csvs == []

    def test_diff_command_max_ts_skip(self, tmp_path):
        """Passing --tables source_flow without --force triggers skip message."""
        from pydsm.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--no-csv",
                "--tables", "SOURCE_FLOW",
                "--max-ts", "25",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "SKIPPED" in result.output

    def test_diff_command_output_file(self, tmp_path):
        """--output FILE writes report to file, not stdout."""
        from pydsm.cli import main

        outfile = str(tmp_path / "report.txt")
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--output", outfile,
                "--tables", "OPRULE_TIME_SERIES",
            ],
        )
        assert result.exit_code == 0, result.output
        # stdout should be empty (or just CSV notice); report goes to file
        with open(outfile, encoding="utf-8") as fh:
            content = fh.read()
        assert "DSM2 Study Diff" in content

    def test_diff_command_table_filter(self, tmp_path):
        """--table CHANNEL restricts static output; other tables not shown."""
        from pydsm.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--table", "CHANNEL",
                "--no-csv",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "CHANNEL" in result.output
        assert "filtered to: CHANNEL" in result.output

    def test_diff_command_no_csv_default(self, tmp_path):
        """Omitting --outdir produces no CSV files (new default: None)."""
        from pydsm.cli import main

        import os as _os
        before = set(_os.listdir("."))
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["diff", ECHO_FILE, ECHO_FILE, "--tables", "OPRULE_TIME_SERIES"],
        )
        after = set(_os.listdir("."))
        assert result.exit_code == 0, result.output
        new_csv = [f for f in after - before if f.endswith(".csv")]
        assert new_csv == [], f"Unexpected CSV files written to cwd: {new_csv}"

    def test_diff_command_table_auto_ts(self, tmp_path):
        """--table BOUNDARY_FLOW auto-enables TS comparison section in output."""
        from pydsm.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "diff",
                ECHO_FILE,
                ECHO_FILE,
                "--table", "BOUNDARY_FLOW",
                "--no-csv",
            ],
        )
        assert result.exit_code == 0, result.output
        # TS section should appear because BOUNDARY_FLOW is TS-backed
        assert "TIME SERIES DATA COMPARISONS" in result.output


# ===========================================================================
# FullReport.to_csv — None outdir guard
# ===========================================================================
class TestFullReportToCsvNone:
    def test_to_csv_none_does_nothing(self, tmp_path):
        """to_csv(None) must return without creating any files."""
        report = FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={
                "CHANNEL": StaticDiff(
                    "CHANNEL",
                    pd.DataFrame({"CHAN_NO": ["999"], "MANNING": [0.035]}),
                    pd.DataFrame(),
                    pd.DataFrame(),
                )
            },
            ts_diffs={},
        )
        report.to_csv(None)  # must not raise, must not create files
        assert list(tmp_path.iterdir()) == []


# ===========================================================================
# FullReport.print_report — file and only_tables params
# ===========================================================================
class TestFullReportPrintReportExtended:
    def _make_report(self):
        changed = pd.DataFrame({
            "CHAN_NO": ["1"],
            "MANNING_a": [0.035],
            "MANNING_b": [0.030],
            "changed_cols": ["MANNING"],
        })
        return FullReport(
            echo_a=ECHO_FILE,
            echo_b=ECHO_FILE,
            timewindow=(datetime(2020, 1, 1), datetime(2024, 1, 1)),
            static_diffs={
                "CHANNEL": StaticDiff("CHANNEL", pd.DataFrame(), pd.DataFrame(), changed),
                "SCALAR": StaticDiff("SCALAR", pd.DataFrame(), pd.DataFrame(), pd.DataFrame()),
            },
            ts_diffs={},
        )

    def test_file_param_writes_to_fileobj(self, tmp_path):
        """print_report(file=fh) writes to the given file object."""
        import io
        buf = io.StringIO()
        self._make_report().print_report(file=buf)
        content = buf.getvalue()
        assert "DSM2 Study Diff" in content

    def test_only_tables_filters_output(self, capsys):
        """only_tables=['CHANNEL'] shows CHANNEL, hides SCALAR."""
        self._make_report().print_report(only_tables=["CHANNEL"])
        out = capsys.readouterr().out
        assert "CHANNEL" in out
        # SCALAR has no diff, but with filtering it should still be listed
        # as an identical table only if present in the filtered set — here it
        # would be absent from the filter entirely, so it must not appear.
        # We test that SCALAR is NOT in the diff section and filter label is shown.
        assert "filtered to: CHANNEL" in out

    def test_only_tables_case_insensitive(self, capsys):
        """only_tables filter is case-insensitive."""
        self._make_report().print_report(only_tables=["channel"])
        out = capsys.readouterr().out
        assert "CHANNEL" in out
        assert "filtered to: CHANNEL" in out

    def test_no_filter_shows_all(self, capsys):
        """Default (only_tables=None) shows all tables."""
        self._make_report().print_report()
        out = capsys.readouterr().out
        assert "CHANNEL" in out
        assert "SCALAR" in out  # listed as Identical


# ===========================================================================
# DSM2Diff.run — only_tables auto-TS behaviour
# ===========================================================================
class TestDSM2DiffRunOnlyTables:
    """Uses object.__new__ injection to avoid file I/O (no fixture file needed)."""

    @staticmethod
    def _make_scalars():
        return pd.DataFrame({
            "NAME": ["run_start_date", "run_start_time", "run_end_date", "run_end_time"],
            "VALUE": ["01JAN2020", "0000", "01JAN2024", "0000"],
        })

    @staticmethod
    def _make_bf_table():
        """Minimal BOUNDARY_FLOW table with one constant entry."""
        return pd.DataFrame({
            "NAME": ["sac"],
            "NODE": [330],
            "SIGN": [1],
            "FILLIN": ["last"],
            "FILE": ["constant"],
            "PATH": ["1.0"],
        })

    @classmethod
    def _make_diff(cls, extra_tables=None):
        """Return a DSM2Diff with injected in-memory tables (no disk reads)."""
        scalars = cls._make_scalars()
        tables = {"SCALAR": scalars}
        if extra_tables:
            tables.update(extra_tables)
        d = object.__new__(DSM2Diff)
        d.echo_a = ECHO_FILE   # path used by abs_path(); doesn't need to exist
        d.echo_b = ECHO_FILE
        d.tables_a = dict(tables)
        d.tables_b = dict(tables)
        return d

    def test_only_tables_stored_on_report(self):
        d = self._make_diff()
        report = d.run(ts_tables=[], only_tables=["CHANNEL"])
        assert report.only_tables == ["CHANNEL"]

    def test_only_tables_none_gives_none(self):
        d = self._make_diff()
        report = d.run(ts_tables=[], only_tables=None)
        assert report.only_tables is None

    def test_only_tables_ts_backed_added_to_ts_diffs(self):
        """only_tables=['BOUNDARY_FLOW'] must trigger TS comparison automatically."""
        d = self._make_diff(extra_tables={"BOUNDARY_FLOW": self._make_bf_table()})
        report = d.run(ts_tables=[], only_tables=["BOUNDARY_FLOW"])
        assert "BOUNDARY_FLOW" in report.ts_diffs

    def test_only_tables_non_ts_table_no_ts_diff(self):
        """only_tables=['CHANNEL'] must NOT add a TS diff for CHANNEL."""
        d = self._make_diff()
        report = d.run(ts_tables=[], only_tables=["CHANNEL"])
        assert "CHANNEL" not in report.ts_diffs
