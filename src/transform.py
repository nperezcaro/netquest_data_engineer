import polars as pl
from typing import Dict

from src.utils import get_logger, is_valid

logger = get_logger(name="TRANSFORM")


def map_non_custom_fields_columns(
    df: pl.DataFrame, channel_dict: Dict[str, str], language_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Modify the Channel and Language columns based on provided mappings.

    Args:
        df (pl.DataFrame): Input DataFrame with Channel and Language columns.
        channel_dict (dict): Mapping dictionary for Channel.
        language_dict (dict): Mapping dictionary for Language.

    Returns:
        pl.DataFrame: DataFrame with new columns 'ChannelB' and 'LanguageB'.

    Raises:
        ValueError: if
            -the provided DataFrame is empty.
            -the provided DataFrame does not contain Channel and Language columns.
            -the provided mappings are not dicts where key and values are strings and not None.
    """
    required_columns = ["Channel", "Language"]

    if df.is_empty():
        error_message = "Provided Dataframe is empty"
        logger.error(error_message)
        raise ValueError(error_message)

    if not all(column in df.columns for column in required_columns):
        error_message = f"Provided Dataframe must contain columns: {required_columns}"
        logger.error(error_message)
        raise ValueError(error_message)

    if not is_valid(channel_dict) or not is_valid(language_dict):
        error_message = (
            "Both Channel and Language dicts should be a non-empty dict of strings"
        )
        logger.error(error_message)
        raise ValueError(error_message)

    return df.with_columns(
        [
            pl.col("Channel").replace(old=channel_dict).alias("ChannelB"),
            pl.col("Language").replace(old=language_dict).alias("LanguageB"),
        ]
    )


def map_custom_fields(df: pl.DataFrame, mapping_dict: Dict[str, str]) -> pl.DataFrame:
    """
    Modify the CustomFields columnn based on provided mapping.

    Args:
        df (pl.DataFrame): Input DataFrame with Channel and Language columns.
        mapping_dict (dict): Mapping dictionary for CustomFields.

    Returns:
        pl.DataFrame: DataFrame with new column 'CustomFieldsB'.

    Raises:
        ValueError: if
            -the provided DataFrame is empty.
            -the provided DataFrame does not contain CustomFields column.
            -the provided mapping is not a dict where key and values are strings and not None.
    """
    required_column = ["CustomFields"]

    if df.is_empty():
        error_message = "Provided Dataframe is empty"
        logger.error(error_message)
        raise ValueError(error_message)

    if not all(column in df.columns for column in required_column):
        error_message = f"Provided Dataframe must contain column: {required_column}"
        logger.error(error_message)
        raise ValueError(error_message)

    if not is_valid(mapping_dict):
        error_message = "CustomField dict should be a non-empty dict of strings"
        logger.error(error_message)
        raise ValueError(error_message)

    return df.with_columns(
        df["CustomFields"]
        .str.split(";")  # Split by ';'
        .list.eval(pl.element().replace(old=mapping_dict))  # Map using the dict
        .list.join(";")  # Join all back into a str
        .alias("CustomFieldsB")
    )


def handle_dimensions(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function aggregates facts for rows where dimensions are equal.

    Args:
        df (pl.DataFrame): Input DataFrame with Duration, id, ChannelB, LanguageB, CustomFieldsB, PointsGained columns.

    Returns:
        pl.DataFrame: DataFrame with aggregated data.

    Raises:
        ValueError: if
            -the provided DataFrame is empty.
            -the provided DataFrame does not contain Duration, id, ChannelB, LanguageB, CustomFieldsB, PointsGained columns.
    """
    required_columns = [
        "Duration",
        "id",
        "ChannelB",
        "LanguageB",
        "CustomFieldsB",
        "PointsGained",
    ]

    if df.is_empty():
        error_message = "Provided Dataframe is empty"
        logger.error(error_message)
        raise ValueError(error_message)

    if not all(column in df.columns for column in required_columns):
        error_message = f"Provided Dataframe must contain column: {required_columns}"
        logger.error(error_message)
        raise ValueError(error_message)

    duration_split_df = df.with_columns(
        # '^(\d+):' captures the digits before the first colon (hours)
        pl.col("Duration").str.extract(r"^(\d+):", 1).cast(pl.Int64).alias("hours"),
        # ':(\d+):' captures the digits between two colons (minutes)
        pl.col("Duration").str.extract(r":(\d+):", 1).cast(pl.Int64).alias("minutes"),
        # ':(\d+)$' captures the digits after the last colon (seconds)
        pl.col("Duration").str.extract(r":(\d+)$", 1).cast(pl.Int64).alias("seconds"),
    )

    total_seconds_df = duration_split_df.with_columns(
        (pl.col("hours") * 3600 + pl.col("minutes") * 60 + pl.col("seconds")).alias(
            "TotalSeconds"
        )
    )

    unique_dimensions_df = total_seconds_df.group_by(
        ["id", "ChannelB", "LanguageB", "CustomFieldsB"], maintain_order=True
    ).agg(
        [
            pl.sum("PointsGained").alias("PointsGained"),
            pl.sum("TotalSeconds").alias("TotalSeconds"),
        ]
    )

    dimensions_duration_df = unique_dimensions_df.with_columns(
        (
            # zfill(2) ensures that each component has two digits ("03" instead of "3")
            # hours only need one digit
            (pl.col("TotalSeconds") // 3600).cast(pl.Int64).cast(pl.Utf8).str.zfill(1)
            + ":"
            + ((pl.col("TotalSeconds") % 3600) // 60)
            .cast(pl.Int64)
            .cast(pl.Utf8)
            .str.zfill(2)
            + ":"
            + (pl.col("TotalSeconds") % 60).cast(pl.Int64).cast(pl.Utf8).str.zfill(2)
        ).alias("Duration")
    )

    result_df = dimensions_duration_df.select(
        ["id", "ChannelB", "LanguageB", "CustomFieldsB", "Duration", "PointsGained"]
    )

    # Replace to the original column names
    return result_df.rename(
        mapping={
            "ChannelB": "Channel",
            "LanguageB": "Language",
            "CustomFieldsB": "CustomFields",
        }
    )


def get_total_points_gained(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function sums the PointsGained over the id column.

    Args:
        df (pl.DataFrame): Input DataFrame with id and PointsGained columns.

    Returns:
        pl.DataFrame: DataFrame with new column 'TotalPointsGained'.

    Raises:
        ValueError: if
            -the provided DataFrame is empty.
            -the provided DataFrame does not contain id and PointsGained columns.
    """
    required_columns = ["id", "PointsGained"]

    if df.is_empty():
        error_message = "Provided Dataframe is empty"
        logger.error(error_message)
        raise ValueError(error_message)

    if not all(column in df.columns for column in required_columns):
        error_message = f"Provided Dataframe must contain column: {required_columns}"
        logger.error(error_message)
        raise ValueError(error_message)

    return df.with_columns(TotalPointsGained=pl.col("PointsGained").sum().over("id"))
