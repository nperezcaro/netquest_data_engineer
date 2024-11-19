import os
import logging
from pathlib import Path

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
)


def get_logger(name: str, level: str = "debug") -> logging.Logger:
    """
    Creates and configures a logger with the given name and level.

    Args:
        name: The name of the logger.
        level: The logging level ("info" or "debug"). Defaults to "debug".

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name=name)

    # Set the logging level
    if level == "info":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    # Attach a console handler, as default one
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(FORMATTER)
        logger.addHandler(console_handler)

    return logger


def obtain_file_path(desired_file: str = "inputs.csv") -> str:
    scrip_dir = Path(os.path.dirname(__file__))

    root = scrip_dir.parent

    target_file_path = root / "data" / desired_file

    if not target_file_path.exists():
        raise FileNotFoundError(f"Target file, {desired_file}, not found.")

    return str(target_file_path.resolve())
