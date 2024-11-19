import os
from pathlib import Path


def obtain_file_path(desired_file: str = "inputs.csv") -> str:
    scrip_dir = Path(os.path.dirname(__file__))

    root = scrip_dir.parent

    target_file_path = root / "data" / desired_file

    if not target_file_path.exists():
        raise FileNotFoundError(f"Target file, {desired_file}, not found.")

    return str(target_file_path.resolve())
