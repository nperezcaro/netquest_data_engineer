import polars as pl
from polars.testing import assert_frame_equal

from src.transform import handle_dimensions


def test_handle_dimensions():
    input_df = pl.DataFrame(
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
            "CustomFieldsB": [
                "area=Accounting;New=true",
                "area=Accounting;New=true",
                "area=Finance;New=false",
                "area=Finance;premium=VIP_User;New=false",
                "area=Customer_Care;New=false",
            ],
        }
    )

    expected_df = pl.DataFrame(
        {
            "id": [1, 1, 2, 3],
            "Channel": ["Channel1", "Channel2", "Channel3", "Channel2"],
            "Language": ["en-US", "en-US", "es-ES", "es-ES"],
            "CustomFields": [
                "area=Accounting;New=true",
                "area=Finance;New=false",
                "area=Finance;premium=VIP_User;New=false",
                "area=Customer_Care;New=false",
            ],
            "Duration": ["01:36:18", "00:37:21", "03:01:47", "01:56:34"],
            "PointsGained": [69, 30, 254, 71],
        }
    )

    output_df = handle_dimensions(df=input_df)

    assert_frame_equal(output_df, expected_df)
