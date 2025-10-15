import os
import re
import numpy as np
import pandas as pd
import pytest

from pydsm.gtmh5 import (
    build_timewindow_for_time,
    get_interpolated_cell_concentrations,
    _DSM2_TIME_FORMAT,
)

TEST_DATA_FILE = os.path.join(
    os.path.dirname(__file__), "data", "hist_v82_mss2_extran_gtm.h5"
)

pytestmark = pytest.mark.skipif(
    not os.path.exists(TEST_DATA_FILE), reason="GTM test data file not present"
)


def test_build_timewindow_for_time_format_and_range():
    target_time = "05FEB2020 0300"
    timewindow, model_time = build_timewindow_for_time(TEST_DATA_FILE, target_time)
    # Expect format START-END with correct pattern
    assert "-" in timewindow
    start_s, end_s = timewindow.split("-")
    pattern = r"^\d{2}[A-Z]{3}\d{4} \d{4}$"
    assert re.match(pattern, start_s)
    assert re.match(pattern, end_s)
    # Model time matches start segment parsed
    parsed_model = pd.to_datetime(start_s, format=_DSM2_TIME_FORMAT)
    assert parsed_model.to_pydatetime() == model_time


def test_get_interpolated_cell_concentrations_shape_and_values():
    target_time = "05FEB2020 0300"
    timewindow, _ = build_timewindow_for_time(TEST_DATA_FILE, target_time)
    concs = get_interpolated_cell_concentrations(
        TEST_DATA_FILE, timewindow, constituent="ec"
    )
    # 2D array shape (1, n_cells)
    assert concs.ndim == 2
    assert concs.shape[0] == 1
    # Values should be finite (non NaN) for at least 90% of cells
    finite_ratio = np.isfinite(concs).sum() / concs.size
    assert finite_ratio > 0.9
