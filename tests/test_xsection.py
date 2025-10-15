import os
import pytest
import pandas as pd
from pathlib import Path
from pydsm.input.parser import read_input


@pytest.fixture
def xsection_data_path():
    """Fixture that returns the path to the test cross-section data file."""
    return Path(__file__).parent / "data" / "test_xsection_data.inp"


@pytest.fixture
def xsection_data(xsection_data_path):
    """Fixture that loads the cross-section data using pydsm input parser."""

    # Parse the input file
    parsed_data = read_input(xsection_data_path)

    # Extract the XSECT_LAYER section
    return parsed_data.get("XSECT_LAYER", [])


def test_xsection_data_file_exists(xsection_data_path):
    """Test that the cross-section data file exists."""
    assert xsection_data_path.exists()


def test_xsection_data_parsing(xsection_data):
    """Test that the cross-section data is parsed correctly."""
    # Check that data was loaded
    assert len(xsection_data) > 0

    # Check first row matches expected format
    first_row = xsection_data.iloc[0, :]
    assert "CHAN_NO" in first_row
    assert "DIST" in first_row
    assert "ELEV" in first_row
    assert "AREA" in first_row
    assert "WIDTH" in first_row
    assert "WET_PERIM" in first_row


def test_xsection_data_values(xsection_data):
    """Test specific values in the cross-section data."""
    # Convert to DataFrame for easier testing
    df = xsection_data

    # Check that we have the right number of rows
    assert len(df) == 11

    # Check that CHAN_NO is consistent
    assert all(df["CHAN_NO"] == 130)

    # Check specific values
    assert df["ELEV"].min() == -10.134
    assert df["ELEV"].max() == 22.832
    assert df["AREA"].max() == 3940.41
