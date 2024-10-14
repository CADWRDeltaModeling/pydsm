# Test reading of hydro and qual echo files into pandas data frames
import os
import pydsm
from pydsm.input import read_input, write_input
from pandas.testing import assert_frame_equal


def test_hydro_input():
    fecho = os.path.join(
        os.path.dirname(__file__), "data", "hydro_echo_historical_v82.inp"
    )
    tables = read_input(fecho)
    assert len(tables.keys()) == 27
    assert list(tables.keys()) == [
        "ENVVAR",
        "SCALAR",
        "IO_FILE",
        "CHANNEL",
        "XSECT",
        "XSECT_LAYER",
        "RESERVOIR",
        "RESERVOIR_VOL",
        "RESERVOIR_CONNECTION",
        "GATE",
        "GATE_WEIR_DEVICE",
        "GATE_PIPE_DEVICE",
        "TRANSFER",
        "CHANNEL_IC",
        "RESERVOIR_IC",
        "BOUNDARY_STAGE",
        "BOUNDARY_FLOW",
        "SOURCE_FLOW",
        "SOURCE_FLOW_RESERVOIR",
        "INPUT_GATE",
        "INPUT_TRANSFER_FLOW",
        "OPERATING_RULE",
        "OPRULE_EXPRESSION",
        "OPRULE_TIME_SERIES",
        "OUTPUT_CHANNEL",
        "OUTPUT_RESERVOIR",
        "OUTPUT_GATE",
    ]
    df = tables["CHANNEL"]
    assert len(df) == 521
    assert len(df[df.LENGTH > 10000]) == 127


#


def test_hydro_write():
    fecho = os.path.join(
        os.path.dirname(__file__), "data", "hydro_echo_historical_v82.inp"
    )
    tables = read_input(fecho)
    fecho2 = os.path.join(
        os.path.dirname(__file__), "data", "hydro_echo_historical_v82_copy.inp"
    )
    if os.path.exists(fecho2):
        os.remove(fecho2)
    write_input(fecho2, tables)
    assert os.path.exists(fecho2)
    tables2 = read_input(fecho2)
    assert len(tables2) == len(tables)
    for name in tables.keys():
        assert_frame_equal(tables[name], tables2[name])
    os.remove(fecho2)


def test_read_output_ec_100():
    """This read was failing for the output table"""
    fecho = os.path.join(os.path.dirname(__file__), "data", "output_stations_ec100.inp")
    tables = read_input(fecho)
    assert len(tables.keys()) == 1
    assert len(tables["OUTPUT_CHANNEL"]) > 100
