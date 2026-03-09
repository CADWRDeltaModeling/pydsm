from pathlib import Path

import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent / "data"


def pytest_addoption(parser):
    parser.addoption(
        "--regen-fixtures",
        action="store_true",
        default=False,
        help=(
            "Regenerate parquet regression fixtures instead of comparing against them. "
            "Use after a deliberate output change or a library upgrade."
        ),
    )


@pytest.fixture
def assert_frame_fixture(request):
    """Compare a DataFrame against a stored parquet snapshot.

    Usage in a test::

        def test_something(self, my_object, assert_frame_fixture):
            df = my_object.compute_something()
            assert_frame_fixture(df, "my_snapshot_name")

    Run ``pytest --regen-fixtures`` to overwrite all snapshots with the
    current output.  The fixture uses Parquet (via pyarrow) rather than
    pickle so that snapshots are stable across numpy/pandas major upgrades.
    """
    regen = request.config.getoption("--regen-fixtures")

    def _check(df: pd.DataFrame, name: str) -> None:
        path = DATA_DIR / f"{name}.parquet"
        if regen:
            df.to_parquet(path)
            return
        expected = pd.read_parquet(path)
        pd.testing.assert_frame_equal(expected, df, check_freq=False)

    return _check
