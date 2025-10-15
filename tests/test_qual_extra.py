import os
import pytest
import pandas as pd
from pydsm.qualh5 import QualH5


@pytest.fixture(scope="module")
def qual():
    filename = os.path.join(os.path.dirname(__file__), "data", "historical_v82_ec.h5")
    return QualH5(filename)


def test_get_output_timedelta_parsing_variants(monkeypatch, qual):
    # Monkeypatch get_output_interval to return a variant that triggers fallback
    monkeypatch.setattr(qual, "get_output_interval", lambda: "60MINUTES")
    td = qual.get_output_timedelta()
    assert td == pd.to_timedelta("60min")


def test_is_gtm_false_for_qual(qual):
    assert not qual.is_gtm()


def test_create_catalog_branching(qual):
    cat = qual.create_catalog()
    # For Qual we expect both CHAN (with UP/DOWN) and CHAN_ (avg) plus RES_
    ids = cat.id.unique()
    assert any(i.startswith("CHAN_") for i in ids)  # avg entries
    assert any(i.startswith("RES_") for i in ids)


def test_names_to_constituent_indices_error(qual):
    with pytest.raises(RuntimeError):
        qual._names_to_constituent_indices(object())  # type: ignore


def test_channel_ids_to_indicies_error(qual):
    with pytest.raises(RuntimeError):
        qual._channel_ids_to_indicies(object())  # type: ignore


def test_channel_locations_to_indicies_error(qual):
    with pytest.raises(RuntimeError):
        qual._channel_locations_to_indicies(object())  # type: ignore
