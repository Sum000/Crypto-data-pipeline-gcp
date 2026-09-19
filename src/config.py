import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

LOCAL_DATA_ROOT = Path(
    os.getenv(
        "LOCAL_DATA_DIR",
        str(PROJECT_ROOT / "data"),
    )
)