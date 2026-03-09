# There are repeating time series that are needed in DSM2. This module
# contains the functions to create them.

import pandas as pd


def create_repeating_timeseries(df_template_year, start_year, end_year):
    freq = df_template_year.index.freq
    if freq == None:
        raise Exception("No frequency defined for template year dataframe")
    # create a dataframe from start_year to end_year with values from the template year dataframe
    df_repeating = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        df_repeating = pd.concat([df_repeating, df_template_year])
    df_repeating.index = pd.period_range(
        start=f"{start_year}-01-01", end=f"{end_year}-12-31", freq=freq
    )
    return df_repeating


def has_data_for_year(data, year):
    eoy = data.index[-1] + pd.offsets.YearEnd(1)
    boy = data.index[-1] + pd.offsets.YearBegin(1)
    num_periods_in_year = pd.date_range(start=boy, end=eoy, freq=data.index.freq).size
    num_periods_for_year = len(data.loc[str(year)])
    return num_periods_for_year == num_periods_in_year


def extend_repeating_timeseries(df_repeating, end_year):
    freq = df_repeating.index.freq
    if freq == None:
        raise Exception("No frequency defined for repeating year dataframe")
    df_last_12 = df_repeating.iloc[-12:]
    last_year = df_last_12.index[-1].year
    if end_year <= last_year:
        raise Exception(
            "end_year must be greater than the last year in the repeating year dataframe"
        )
    # select last year in the repeating dataframe
    if freq != "ME":
        raise Exception("Only monthly frequency is supported")
    # repeat last 12 months of data till end_year
    df_extended = df_repeating.copy()
    for year in range(last_year, end_year):
        df_extended = pd.concat([df_extended, df_last_12])
    # if data frame index is period, then create period range
    if isinstance(df_extended.index, pd.PeriodIndex):
        df_extended.index = pd.period_range(
            start=df_extended.index[0], periods=len(df_extended), freq=freq
        )
    return df_extended

    nyears = df_repeating.index[-1].year - end_year
    nmonths = nyears * 12
    #
    df_repeating.shift(periods=12, freq="M")

    # Find the last complete year in the series
    start_year = df_repeating.index[-1].year
    earliest_year = df_repeating.index[0].year
    while (
        not has_data_for_year(df_repeating, start_year) and start_year > earliest_year
    ):
        start_year -= 1
    # Trim the repeating dataframe to the last complete year
    df_last_year = df_repeating.loc[f"{start_year}-01-01":f"{start_year}-12-31"]
    # create a dataframe from start_year to end_year with values from the repeating year dataframe
    df_extended = pd.DataFrame()  # create an empty dataframe
    for year in range(start_year, end_year + 1):
        df_extended = df_extended.append(df_last_year)
    # if data frame index is period, then create period range
    if isinstance(df_extended.index, pd.PeriodIndex):
        df_extended.index = pd.period_range(
            start=df_last_year.index[0], end=f"{end_year}-12-31", freq=freq
        )
    else:
        df_extended.index = pd.date_range(
            start=df_last_year.index[0], end=f"{end_year}-12-31", freq=freq
        )
    return df_extended
