import os
import polars as pl


def get_df_from_csv(file_path: str, separator: str = ";") -> pl.DataFrame:
    if not os.path.exists(path=file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pl.read_csv(source=file_path, separator=separator)
