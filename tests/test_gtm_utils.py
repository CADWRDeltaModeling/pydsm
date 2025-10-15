import os
import pytest
import pandas as pd
from datetime import datetime
from pydsm.gtmh5 import (
    _nearest_time,
    _format_time,
    _parse_dsm2_time,
    build_timewindow_for_time,
    get_interpolated_cell_concentrations,
)
from pydsm.qualh5 import QualH5


@pytest.fixture(scope="module")
def gtm_file():
    return os.path.join(
        os.path.dirname(__file__), "data", "hist_v82_mss2_extran_gtm.h5"
    )


def test_parse_and_format_time_roundtrip():
    s = "05FEB2020 0300"
    dt = _parse_dsm2_time(s)
    assert _format_time(dt) == s


def test_nearest_time_clamp_edges():
    start = datetime(2020, 1, 15, 0, 0)
    end = datetime(2020, 1, 31, 0, 0)
    freq = "60min"
    # before start -> start
    assert _nearest_time("01JAN2020 0000", start, end, freq) == start
    # after end -> end
    assert _nearest_time("10FEB2020 0000", start, end, freq) == end


def test_build_timewindow_for_time(gtm_file):
    tw, model_time = build_timewindow_for_time(gtm_file, "20JAN2020 0530")
    # timewindow pattern
    assert "-" in tw
    assert isinstance(model_time, datetime)


def test_get_interpolated_cell_concentrations_shape(gtm_file):
    # Build a 1-interval timewindow anchored to model start
    q = QualH5(gtm_file)
    start, end = q.get_start_end_dates()
    freq = q.get_output_freq()
    start_dt = pd.to_datetime(start)
    tw = f"{start_dt.strftime('%d%b%Y %H%M')}-{(start_dt+freq).strftime('%d%b%Y %H%M')}"
    conc = get_interpolated_cell_concentrations(gtm_file, tw, constituent="ec")
    assert conc.ndim == 2 and conc.shape[0] == 1
    assert conc.shape[1] > 100  # expect many cells
