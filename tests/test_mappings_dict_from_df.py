import pytest
import polars as pl
from src.utils import obtain_file_path
from src.extract import get_mappings_dict, filter_and_get_dict


@pytest.fixture
def mappings_df():
    path = obtain_file_path(desired_file="mappings.csv")
    df = pl.read_csv(source=path, separator=";")

    return df


def test_filter_and_get_dict(mappings_df):
    df = mappings_df

    result = filter_and_get_dict(df=df, field_name="Channel")
    expected = {"channel1": "Channel1", "channel2": "Channel2", "channel3": "Channel3"}

    assert result == expected


def test_dict_build(mappings_df):
    df = mappings_df

    expected_channel_dict = {
        "channel1": "Channel1",
        "channel2": "Channel2",
        "channel3": "Channel3",
    }
    expected_language_dict = {"en": "en-US", "en_us": "en-US", "es": "es-ES"}
    excepted_customfields_dict = {
        "Area=account": "area=Accounting",
        "Area=finance": "area=Finance",
        "Area=customer": "area=Customer_Care",
        "Premium=premium-user": "premium=VIP_User",
    }

    result = get_mappings_dict(df=df)

    assert result == (
        expected_channel_dict,
        expected_language_dict,
        excepted_customfields_dict,
    )


def test_empty_df():
    df = pl.DataFrame()

    with pytest.raises(ValueError):
        get_mappings_dict(df=df)


def test_missing_columns():
    data = {
        "Field": ["Channel", "Language", "CustomFields"],
        "SoftwareA": ["channel1", "en", "Area=account"],
        # Missing "SoftwareB" column
    }

    df = pl.DataFrame(data)

    with pytest.raises(ValueError):
        get_mappings_dict(df=df)
