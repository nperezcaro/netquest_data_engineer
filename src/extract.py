import os
import polars as pl
from typing import Tuple, Dict

from src.utils import get_logger

logger = get_logger(name="EXTRACT")


def get_df_from_csv(file_path: str, separator: str = ";") -> pl.DataFrame:
    """
    This function takes a path to a CSV file and returns a Polars DataFrame.

    Args:
        file_path: The path to the CSV file intended to be used.
        separator: The delimeter used in the CSV file. Defaults to ";".

    Returns:
        pl.DataFrame: A DataFrame that contains the CSV data.

    Raises:
        FileNotFoundError: if the provided path leads to a file that does not exist.
    """
    if not os.path.exists(path=file_path):
        e = f"File not found: {file_path}"
        logger.error(e)
        raise FileNotFoundError(e)
    return pl.read_csv(source=file_path, separator=separator)


def filter_and_get_dict(df: pl.DataFrame, field_name: str) -> Dict[str, str]:
    filtered_df = df.filter(pl.col("Field") == field_name)
    return {row["SoftwareA"]: row["SoftwareB"] for row in filtered_df.to_dicts()}


def get_mappings_dict(
    df: pl.DataFrame,
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    required_columns = ["Field", "SoftwareA", "SoftwareB"]

    if df.is_empty():
        error_message = "Provided Dataframe is empty"
        logger.error(error_message)
        raise ValueError(error_message)

    if not all(column in df.columns for column in required_columns):
        error_message = f"Provided Dataframe must contain columns: {required_columns}"
        logger.error(error_message)
        raise ValueError(error_message)

    channel_mapping = filter_and_get_dict(df=df, field_name="Channel")
    language_mapping = filter_and_get_dict(df=df, field_name="Language")
    customfields_mapping = filter_and_get_dict(df=df, field_name="CustomFields")

    return channel_mapping, language_mapping, customfields_mapping
