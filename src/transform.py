import polars as pl
from typing import Dict

from src.utils import get_logger, is_valid

logger = get_logger(name="TRANSFORM")


def map_non_custom_fields_columns(
    df: pl.DataFrame, channel_dict: Dict[str, str], language_dict: Dict[str, str]
) -> pl.DataFrame:
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
