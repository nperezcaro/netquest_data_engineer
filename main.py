import polars as pl
import sys

from src.utils import obtain_file_path, get_logger
from src.extract import get_df_from_csv, get_mappings_dict
from src.transform import (
    map_non_custom_fields_columns,
    map_custom_fields,
    handle_dimensions,
    get_total_points_gained,
)

logger = get_logger(name="MAIN")


def run() -> pl.DataFrame:
    try:
        logger.info("extracting input and mappings")
        inputs_df = get_df_from_csv(file_path=obtain_file_path())
        mappings_df = get_df_from_csv(
            file_path=obtain_file_path(desired_file="mappings.csv")
        )
        logger.info("input and mappings extraction DONE")

        logger.info("getting mappings dicts")
        channel_mapping_dict, language_mapping_dict, customfields_mapping_dict = (
            get_mappings_dict(df=mappings_df)
        )
        logger.info("mappings dicts generation DONE")

        logger.info("mapping channel and language changes")
        channel_language_updated_df = map_non_custom_fields_columns(
            df=inputs_df,
            channel_dict=channel_mapping_dict,
            language_dict=language_mapping_dict,
        )
        logger.info("channel and language changes mapping DONE")

        logger.info("mapping custom fields changes")
        all_dimensions_updated_df = map_custom_fields(
            df=channel_language_updated_df, mapping_dict=customfields_mapping_dict
        )
        logger.info("custom fields changes mapping DONE")

        logger.info("aggregating duplicate dimensions")
        unique_dimensions_df = handle_dimensions(df=all_dimensions_updated_df)
        logger.info("duplicate dimensions aggregation DONE")

        logger.info("calculating total points gained")
        result_df = get_total_points_gained(df=unique_dimensions_df)
        logger.info("total points gained calculation DONE")

        logger.info("SUCCESS")

        return result_df
    except Exception as e:
        logger.error(f"An error ocurred:{e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    df = run()
