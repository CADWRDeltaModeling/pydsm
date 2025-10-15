import os
import pytest
import pandas as pd
from pydsm.hydroh5 import HydroH5


@pytest.fixture(scope="module")
def hydro():
    filename = os.path.join(os.path.dirname(__file__), "data", "historical_v82.h5")
    return HydroH5(filename)


def test_create_catalog_and_fetch_entries(hydro):
    catalog = hydro.create_catalog()
    # basic expectations: contains multiple variable types
    vars_in_cat = set(catalog.variable.str.upper().unique())
    assert {"FLOW", "AREA", "STAGE", "HEIGHT"}.issubset(vars_in_cat)
    # pick a few representative entries (first 5) and ensure data can be retrieved
    sample = catalog.head(5)
    for _, row in sample.iterrows():
        df = hydro.get_data_for_catalog_entry(row)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


def test_get_data_for_catalog_entry_specific_flows(hydro):
    catalog = hydro.create_catalog()
    # channel flow upstream entry
    chan_flow_entry = catalog[
        catalog.id.str.contains("CHAN_1_UP") & (catalog.variable == "flow")
    ].iloc[0]
    flow_df = hydro.get_data_for_catalog_entry(chan_flow_entry)
    assert "1-upstream" in flow_df.columns[0].lower()
    # channel avg area (no up/down)
    chan_avg_area_entry = catalog[
        (catalog.variable == "area") & (~catalog.id.str.contains("UP"))
    ].iloc[0]
    area_df = hydro.get_data_for_catalog_entry(chan_avg_area_entry)
    assert area_df.shape[1] == 1


def test_get_data_for_catalog_entry_qext_and_reservoir(hydro):
    catalog = hydro.create_catalog()
    qext_entries = catalog[catalog.id.str.startswith("QEXT_")]
    if not qext_entries.empty:
        qext_entry = qext_entries.iloc[0]
        q_df = hydro.get_data_for_catalog_entry(qext_entry)
        assert not q_df.empty
    res_entries = catalog[
        catalog.id.str.startswith("RES_") & (catalog.variable == "height")
    ]
    if not res_entries.empty:
        res_entry = res_entries.iloc[0]
        r_df = hydro.get_data_for_catalog_entry(res_entry)
        assert not r_df.empty


def test_channel_ids_to_indicies_errors(hydro):
    # pass an invalid type to trigger error path
    with pytest.raises(RuntimeError):
        hydro._channel_ids_to_indicies(object())  # type: ignore


def test_channel_locations_to_indicies_error(hydro):
    with pytest.raises(RuntimeError):
        hydro._channel_locations_to_indicies(object())  # type: ignore


def test_get_channel_bottom_single_and_list(hydro):
    # single channel
    df_single = hydro.get_channel_bottom("4")
    assert list(df_single.columns) == ["upstream", "downstream"]
    # list form
    df_list = hydro.get_channel_bottom(["4", "5"])
    assert df_list.shape[0] == 2


def test_get_channel_flow_all(hydro):
    df = hydro.get_channel_flow("all", "upstream")
    # Should have one column per channel (at least > 400 for test data set) and many rows
    assert df.shape[1] > 400
    assert df.shape[0] > 100  # time rows
