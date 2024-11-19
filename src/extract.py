import polars as pl


def get_df_from_csv(file_path: str, separator: str = ";") -> pl.DataFrame:
    return pl.read_csv(source=file_path, separator=separator)
