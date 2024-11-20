import pytest
import polars as pl
from polars.testing import assert_frame_equal
from src.utils import obtain_file_path
from src.transform import map_custom_fields


@pytest.fixture
def input_df():
    path = obtain_file_path()
    df = pl.read_csv(source=path, separator=";")

    return df


@pytest.fixture
def custom_fields_mapping():
    return {
        "Area=account": "area=Accounting",
        "Area=finance": "area=Finance",
        "Area=customer": "area=Customer_Care",
        "Premium=premium-user": "premium=VIP_User",
    }


def test_modify_custom_fields(input_df, custom_fields_mapping):
    excepted_df = pl.DataFrame(
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
            "CustomFieldsB": [
                "area=Accounting;New=true",
                "area=Accounting;New=true",
                "area=Finance;New=false",
                "area=Finance;premium=VIP_User;New=false",
                "area=Customer_Care;New=false",
            ],
        }
    )

    result_df = map_custom_fields(df=input_df, mapping_dict=custom_fields_mapping)

    assert_frame_equal(result_df, excepted_df)


def test_empty_df(custom_fields_mapping):
    df = pl.DataFrame()

    with pytest.raises(ValueError):
        map_custom_fields(df=df, mapping_dict=custom_fields_mapping)
