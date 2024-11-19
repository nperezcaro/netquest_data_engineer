import os
import polars as pl


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
        raise FileNotFoundError(f"File not found: {file_path}")
    return pl.read_csv(source=file_path, separator=separator)
