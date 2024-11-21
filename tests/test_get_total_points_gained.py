import pytest
import polars as pl
from polars.testing import assert_frame_equal

from src.transform import get_total_points_gained


def test_get_total_points_gained():
    input_df = pl.DataFrame(
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

    excepted_df = pl.DataFrame(
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
            "TotalPointsGained": [99, 99, 254, 71],
        }
    )

    result_df = get_total_points_gained(df=input_df)

    assert_frame_equal(result_df, excepted_df)


def test_empty_df():
    df = pl.DataFrame()

    with pytest.raises(ValueError):
        get_total_points_gained(df=df)
