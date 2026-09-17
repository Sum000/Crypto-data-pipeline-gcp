from src.quality.validation import (
    validate_transformed_data,
)
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.storage.gcs import (
    download_file_from_gcs,
    get_latest_raw_object,
    upload_file_to_gcs,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
TRANSFORMED_DATA_DIR = PROJECT_ROOT / "data" / "transformed"


def load_raw_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def transform_data(data, ingestion_timestamp):
    df = pd.DataFrame(data)

    df = df[
        [
            "id",
            "symbol",
            "name",
            "market_cap_rank",
            "current_price",
            "market_cap",
            "total_volume",
            "high_24h",
            "low_24h",
            "price_change_24h",
            "price_change_percentage_24h",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "last_updated",
        ]
    ]

    df = df.rename(
        columns={
            "id": "crypto_id",
            "current_price": "current_price_usd",
            "market_cap": "market_cap_usd",
            "total_volume": "total_volume_usd",
            "high_24h": "high_24h_usd",
            "low_24h": "low_24h_usd",
            "price_change_24h": "price_change_24h_usd",
            "price_change_percentage_24h": "price_change_24h_pct",
        }
    )

    df["ingestion_timestamp"] = ingestion_timestamp

    return df


def save_transformed_data(df):
    TRANSFORMED_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    output_file = (
        TRANSFORMED_DATA_DIR
        / f"crypto_market_{timestamp}.csv"
    )

    df.to_csv(
        output_file,
        index=False,
    )

    print(
        f"Transformed data saved locally: {output_file}"
    )

    gcs_object_name = (
        f"transformed/crypto/"
        f"{timestamp[:4]}/"
        f"{timestamp[4:6]}/"
        f"{timestamp[6:8]}/"
        f"crypto_market_{timestamp}.csv"
    )

    gcs_uri = upload_file_to_gcs(
    output_file,
    gcs_object_name,

    )

    return gcs_uri


def process_raw_object(raw_object_name):
    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    local_raw_file = (
        RAW_DATA_DIR
        / Path(raw_object_name).name
    )

    download_file_from_gcs(
        raw_object_name,
        local_raw_file,
    )

    data = load_raw_data(local_raw_file)

    ingestion_timestamp = get_ingestion_timestamp(
    raw_object_name
    )

    df = transform_data(
    data,
    ingestion_timestamp,
    )

    



    print("\nTransformed data:")
    print(df)

    print("\nRunning data quality checks...")

    validate_transformed_data(df)

    transformed_gcs_uri = save_transformed_data(df)

    return transformed_gcs_uri


def get_ingestion_timestamp(raw_object_name):
    filename = Path(raw_object_name).stem

    timestamp_string = filename.replace(
        "crypto_",
        ""
    )

    ingestion_timestamp = datetime.strptime(
        timestamp_string,
        "%Y%m%dT%H%M%SZ"
    ).replace(tzinfo=timezone.utc)

    return ingestion_timestamp


if __name__ == "__main__":
    raw_object = get_latest_raw_object()

    print(f"Latest GCS raw object: {raw_object}")

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    local_raw_file = RAW_DATA_DIR / "latest_raw.json"

    download_file_from_gcs(
        raw_object,
        local_raw_file,
    )

    data = load_raw_data(local_raw_file)

    df = transform_data(data)

    print("\nTransformed data:")
    print(df)

    transformed_gcs_uri = save_transformed_data(df)

    print(
        f"Transformed GCS URI: "
        f"{transformed_gcs_uri}"
    )
