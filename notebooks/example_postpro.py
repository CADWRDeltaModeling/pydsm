# %%
import pydsm
from pydsm import postpro
import pytest
import numpy as np
import pandas as pd

# %%
study = postpro.Study(
    "observed",
    "D:/temp/postprocessingTestFolder/postprocessing/observed_data/cdec/ec_merged.dss",
)
loc = postpro.Location(
    "Old River",
    "OLD",
    "Old River at Tracy",
    time_window_exclusion_list=None,
    threshold_value=0,
)
vtype = postpro.VarType("EC", "mmhos/cm")
p = postpro.PostProcessor(study, loc, vtype)
# %%
# import cProfile
# cProfile.run("p.process()", "postpro.prof")
# %%
from vtools.functions.lag_cross_correlation import calculate_lag
calculate_lag()
# %%
p.process()
# %%
# %%
assert p.df is not None
p.store_processed()
p2 = postpro.PostProcessor(study, loc, vtype)
p2.load_processed(timewindow="03OCT2016 0000 - 05OCT2016 0000")
assert p2.df.index[0] == pd.to_datetime("03OCT2016 0000")
assert p2.df.index[-1] == pd.to_datetime("05OCT2016 0000")
# assert_frame_equal(p.df, p2.df,check_names=False, check_column_type=False, check_like=False)
