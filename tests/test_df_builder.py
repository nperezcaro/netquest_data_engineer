import pytest
import polars as pl
from src.extract import get_df_from_csv
from src.utils import obtain_file_path


@pytest.fixture
def valid_csv():
    return obtain_file_path()


def test_load_valid_csv(valid_csv):
    df = get_df_from_csv(valid_csv)
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (5, 6)


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        get_df_from_csv(file_path="non_existing_file.csv")
