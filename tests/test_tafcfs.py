import pytest
from pydsm.functions.tafcfs import tafm2cfs, cfs2tafm
import pandas as pd
import numpy as np
from pathlib import Path


@pytest.fixture
def cfs2_taf(request):
    # Project root (pytest sets this)
    root = request.config.rootpath
    # Prefer data under project root
    csv_path = root / "data" / "cfs2_taf.csv"
    if not csv_path.is_file():
        # Fallback: tests/data
        alt = Path(__file__).parent / "data" / "cfs2_taf.csv"
        if alt.is_file():
            csv_path = alt
        else:
            pytest.skip(f"Missing expected CSV at {csv_path} or {alt}")
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    return df.to_period(freq="M")


def test_cfs2taf(cfs2_taf):
    cfs2_taf = cfs2_taf[:"2003"]
    c2t = cfs2tafm()
    x = pd.concat([cfs2_taf, c2t], axis=1).dropna()
    assert np.allclose(x.iloc[:, 0], x.iloc[:, 1])
