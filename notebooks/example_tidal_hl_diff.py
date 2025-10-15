# %%
from pydsm.functions import tidalhl
import os
import pandas as pd


def tidal_signal_with_gaps():
    """
    A simple tidal signal fixture
    """
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "data", "tidal_signal_with_gaps.csv"),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfh = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_gaps_highs.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfl = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_gaps_lows.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    return df, dfh, dfl


# %%
df, dfh, dfl = tidal_signal_with_gaps()
# %%
dfh1 = dfh.copy()
dfh2 = dfh.copy()
dfh2 = 1.1 * dfh
dfh2.index = dfh.index + pd.Timedelta("22m")
dfh2
# %%
pd.merge(dfh, dfh2, left_index=True, right_index=True, how="outer")

# %%
pd.merge_asof(
    dfh,
    dfh2,
    left_index=True,
    right_index=True,
    direction="nearest",
    tolerance=pd.Timedelta("2h"),
)
# %%
dfh2[dfh2.index.name] = dfh2.index
dfh[dfh.index.name] = dfh.index
# %%
dfm = pd.merge_asof(
    dfh,
    dfh2,
    left_index=True,
    right_index=True,
    direction="nearest",
    tolerance=pd.Timedelta("2h"),
)

# %%
dfampdiff = dfm["max_x"] - dfm["max_y"]
# %%
dfphasediff = dfm["time_x"] - dfm["time_y"]
# %%
dfampdiff
# %%
dfphasediff.apply(lambda x: x.total_seconds() / 60)
