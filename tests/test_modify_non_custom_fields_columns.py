import pytest
import polars as pl
from polars.testing import assert_frame_equal
from src.utils import obtain_file_path
from src.transform import map_non_custom_fields_columns


@pytest.fixture
def input_df():
    path = obtain_file_path()
    df = pl.read_csv(source=path, separator=";")

    return df


def test_modify_non_custom_fields_columns(input_df):
    expected_df = pl.DataFrame(
        {
            "id": [1, 1, 1, 2, 3],
            "Channel": ["channel1", "channel1", "channel2", "channel3", "channel2"],
            "Language": ["en", "en_us", "en", "es", "es"],
            "CustomFields": [
                "Area=account;New=true",
                "Area=account;New=true",
                "Area=finance;New=false",
                "Area=finance;Premium=premium-user;New=false",
                "Area=customer;New=false",
            ],
            "Duration": ["1:23:14", "0:13:04", "0:37:21", "3:01:47", "1:56:34"],
            "PointsGained": [57, 12, 30, 254, 71],
            "ChannelB": ["Channel1", "Channel1", "Channel2", "Channel3", "Channel2"],
            "LanguageB": ["en-US", "en-US", "en-US", "es-ES", "es-ES"],
        }
    )

    channel_dict = {
        "channel1": "Channel1",
        "channel2": "Channel2",
        "channel3": "Channel3",
    }
    language_dict = {"en": "en-US", "en_us": "en-US", "es": "es-ES"}

    result_df = map_non_custom_fields_columns(
        df=input_df, channel_dict=channel_dict, language_dict=language_dict
    )

    assert_frame_equal(result_df, expected_df)


def test_empty_df():
    df = pl.DataFrame()

    channel_dict = {
        "channel1": "Channel1",
        "channel2": "Channel2",
        "channel3": "Channel3",
    }
    language_dict = {"en": "en-US", "en_us": "en-US", "es": "es-ES"}

    with pytest.raises(ValueError):
        map_non_custom_fields_columns(
            df=df, channel_dict=channel_dict, language_dict=language_dict
        )


def test_missing_columns():
    data = {
        "id": [1, 1, 1, 2, 3],
        # Missing "Channel" column
        "Language": ["en", "en_us", "en", "es", "es"],
        "CustomFields": [
            "Area=account;New=true",
            "Area=account;New=true",
            "Area=finance;New=false",
            "Area=finance;Premium=premium-user;New=false",
            "Area=customer;New=false",
        ],
        "Duration": ["1:23:14", "0:13:04", "0:37:21", "3:01:47", "1:56:34"],
        "PointsGained": [57, 12, 30, 254, 71],
    }

    df = pl.DataFrame(data)

    channel_dict = {
        "channel1": "Channel1",
        "channel2": "Channel2",
        "channel3": "Channel3",
    }
    language_dict = {"en": "en-US", "en_us": "en-US", "es": "es-ES"}

    with pytest.raises(ValueError):
        map_non_custom_fields_columns(
            df=df, channel_dict=channel_dict, language_dict=language_dict
        )


def test_invalid_dict():
    data = {
        "id": [1, 1, 1, 2, 3],
        "Channel": ["channel1", "channel1", "channel2", "channel3", "channel2"],
        "Language": ["en", "en_us", "en", "es", "es"],
        "CustomFields": [
            "Area=account;New=true",
            "Area=account;New=true",
            "Area=finance;New=false",
            "Area=finance;Premium=premium-user;New=false",
            "Area=customer;New=false",
        ],
        "Duration": ["1:23:14", "0:13:04", "0:37:21", "3:01:47", "1:56:34"],
        "PointsGained": [57, 12, 30, 254, 71],
    }

    df = pl.DataFrame(data)

    channel_dict = {
        1: "Channel1",
        "channel2": "Channel2",
        "channel3": "Channel3",
    }
    language_dict = {"en": False, "en_us": "en-US", "es": "es-ES"}

    with pytest.raises(ValueError):
        map_non_custom_fields_columns(
            df=df, channel_dict=channel_dict, language_dict=language_dict
        )
