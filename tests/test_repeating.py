import pandas as pd
import pytest

from pydsm.repeating_timeseries import create_repeating_timeseries, extend_repeating_timeseries

# Sample template year dataframe
values = list(range(1,13))
df_template_year = pd.DataFrame({'value': values}, index=pd.date_range('2000-01-01', periods=12, freq='M'))

def test_create_repeating_timeseries():
    # Test case 1: Create repeating timeseries for two years
    df_repeating = create_repeating_timeseries(df_template_year, 2000, 2001)
    assert len(df_repeating) == 24  # The length of the resulting dataframe should be 24 (2 years)
    assert df_repeating['value'].tolist() == values * 2  # The values in the resulting dataframe should match the template year dataframe

    # Test case 2: Create repeating timeseries for one year
    df_repeating = create_repeating_timeseries(df_template_year, 2000, 2000)
    assert len(df_repeating) == 12  # The length of the resulting dataframe should be 12 (1 year)
    assert df_repeating['value'].tolist() == values  # The values in the resulting dataframe should match the template year dataframe

def test_extend_repeating_timeseries():
    # Sample repeating timeseries dataframe
    df_repeating = create_repeating_timeseries(df_template_year, 2000, 2001)

    # Test case 1: Extend repeating timeseries for two more years
    df_extended = extend_repeating_timeseries(df_repeating, 2003)
    assert len(df_extended) == 48  # The length of the resulting dataframe should be 48 (4 years)
    assert df_extended['value'].tolist() == values * 4  # The values in the resulting dataframe should match the repeating dataframe

    # Test case 2: Extend repeating timeseries for one more year
    df_extended = extend_repeating_timeseries(df_repeating, 2002)
    assert len(df_extended) == 36  # The length of the resulting dataframe should be 36 (3 years)
    assert df_extended['value'].tolist() == values * 3  # The values in the resulting dataframe should match the repeating dataframe

