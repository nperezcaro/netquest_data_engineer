import pytest
from polars.testing import assert_frame_equal

from src.utils import obtain_file_path
from src.extract import get_df_from_csv
from main import run


@pytest.fixture
def outputs_df():
    path = obtain_file_path(desired_file="outputs.csv")
    df = get_df_from_csv(file_path=path)

    return df


def test_main(outputs_df):
    result_df = run()

    assert_frame_equal(result_df, outputs_df)
