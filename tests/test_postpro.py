from pydsm.analysis import postpro
import pytest
import numpy as np
import pandas as pd
from pydsm.analysis.postpro import convert_index_to_timestamps, merge


def test_postprocessor():
    study = postpro.Study("observed", "data/sample_obs_small.dss")
    loc = postpro.Location(
        "GrantLineCanal",
        "GLC",
        "Grant Line Canal in South Delta",
        time_window_exclusion_list=None,
        threshold_value=0,
    )
    vtype = postpro.VarType("ec", "mmhos/cm")
    p = postpro.PostProcessor(study, loc, vtype)
    p.process()
    assert p.df is not None
    p.store_processed()
    p2 = postpro.PostProcessor(study, loc, vtype)
    p2.load_processed(timewindow="03OCT2016 0000 - 05OCT2016 0000")
    assert p2.df.index[0] == pd.to_datetime("03OCT2016 0000")
    assert p2.df.index[-1] == pd.to_datetime("05OCT2016 0000")
    # assert_frame_equal(p.df, p2.df,check_names=False, check_column_type=False, check_like=False)


# ---------------------------------------------------------------------------
# convert_index_to_timestamps — datetime resolution normalisation
# ---------------------------------------------------------------------------

class TestConvertIndexToTimestamps:
    """Regression: convert_index_to_timestamps must coerce datetime64[us] to
    datetime64[ns] — previously it returned early without coercing because the
    index was already a DatetimeIndex."""

    def _make(self, dtype, n=10):
        idx = pd.date_range("2020-01-01", periods=n, freq="15min").astype(dtype)
        return pd.DataFrame({"v": range(n)}, index=idx)

    def test_ns_index_unchanged(self):
        df = self._make("datetime64[ns]")
        convert_index_to_timestamps(df)
        assert df.index.dtype == np.dtype("datetime64[ns]")

    def test_us_index_coerced_to_ns(self):
        """Before the fix this was a no-op; the index stayed as datetime64[us]."""
        df = self._make("datetime64[us]")
        convert_index_to_timestamps(df)
        assert df.index.dtype == np.dtype("datetime64[ns]"), (
            f"Expected datetime64[ns], got {df.index.dtype}"
        )

    def test_period_index_converted(self):
        idx = pd.period_range("2020-01-01", periods=10, freq="15min")
        df = pd.DataFrame({"v": range(10)}, index=idx)
        convert_index_to_timestamps(df)
        assert isinstance(df.index, pd.DatetimeIndex)


# ---------------------------------------------------------------------------
# merge() — mixed datetime resolution (ns vs us)
# ---------------------------------------------------------------------------

class TestMergeMixedResolution:
    """merge() calls combine_first() which requires aligned indices.
    Both inputs must be normalised to the same resolution first."""

    def _make(self, dtype, start="2020-01-01", n=20, col="v"):
        idx = pd.date_range(start, periods=n, freq="15min").astype(dtype)
        return pd.DataFrame({col: np.random.default_rng(0).random(n)}, index=idx)

    def test_ns_plus_us_does_not_raise(self):
        df_ns = self._make("datetime64[ns]", start="2020-01-01")
        df_us = self._make("datetime64[us]", start="2020-01-01 05:00")  # overlapping
        result = merge([df_ns, df_us])
        assert result is not None
        assert isinstance(result.index, pd.DatetimeIndex)
        assert result.index.dtype == np.dtype("datetime64[ns]")

    def test_both_us_does_not_raise(self):
        df1 = self._make("datetime64[us]", start="2020-01-01")
        df2 = self._make("datetime64[us]", start="2020-01-01 05:00")
        result = merge([df1, df2])
        assert result is not None
        assert result.index.dtype == np.dtype("datetime64[ns]")

    def test_result_spans_both_inputs(self):
        df_ns = self._make("datetime64[ns]", start="2020-01-01", n=4)
        df_us = self._make("datetime64[us]", start="2020-01-01 01:00", n=4)
        result = merge([df_ns, df_us])
        # Result should cover the full range of both inputs
        assert result.index.min() <= pd.Timestamp("2020-01-01")
        assert result.index.max() >= pd.Timestamp("2020-01-01 01:45")
