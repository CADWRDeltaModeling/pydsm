# Test reading of hydro and qual echo files into pandas data frames
import pydsm
from pydsm.input import parser


def test_hydro_read():
    fecho = 'hydro_echo_historical_v82.inp'
    with open(fecho, 'r') as f:
        tables = parser.parse(f.read())
    assert len(tables.keys()) == 27
    assert list(tables.keys()) == ['ENVVAR', 'SCALAR', 'IO_FILE',
                             'CHANNEL', 'XSECT', 'XSECT_LAYER',
                             'RESERVOIR', 'RESERVOIR_VOL', 'RESERVOIR_CONNECTION',
                             'GATE', 'GATE_WEIR_DEVICE', 'GATE_PIPE_DEVICE', 'TRANSFER',
                             'CHANNEL_IC', 'RESERVOIR_IC',
                             'BOUNDARY_STAGE', 'BOUNDARY_FLOW', 'SOURCE_FLOW', 'SOURCE_FLOW_RESERVOIR',
                             'INPUT_GATE', 'INPUT_TRANSFER_FLOW',
                             'OPERATING_RULE', 'OPRULE_EXPRESSION', 'OPRULE_TIME_SERIES',
                             'OUTPUT_CHANNEL', 'OUTPUT_RESERVOIR', 'OUTPUT_GATE']
    df = tables['CHANNEL']
    assert len(df) == 521
    assert len(df[df.LENGTH > 10000]) == 127
#

def test_hydro_network():
    pass
