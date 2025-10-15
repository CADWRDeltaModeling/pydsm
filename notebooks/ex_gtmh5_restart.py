# %%
from logging import warning
import pydsm
from pydsm import dsm2h5
from pydsm import qualh5
import pandas as pd
import numpy as np
from datetime import datetime
import h5py

# Import shared GTM helper utilities
from pydsm.gtmh5 import (
    build_timewindow_for_time,
    get_interpolated_cell_concentrations,
)


# %%
tidefile = "data/hist_v82_mss2_extran_gtm.h5"
# Example target time (previously inside a window)
target_time = "05FEB2020 0300"

# Build timewindow separately
timewindow, chosen_time = build_timewindow_for_time(tidefile, target_time)

# Interpolate using the derived timewindow
conc_array = get_interpolated_cell_concentrations(
    tidefile, timewindow, constituent="ec"
)
conc_array
# %%
