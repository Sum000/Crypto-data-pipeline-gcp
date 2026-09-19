from src.config import LOCAL_DATA_ROOT
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.storage.gcs import (
    download_file_from_gcs,
    upload_file_to_gcs,
)


# PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = (
    LOCAL_DATA_ROOT
    / "raw"
    / "historical"
)

TRANSFORMED_DATA_DIR = (
    LOCAL_DATA_ROOT
    / "transformed"
    / "historical"
)


def load_historical_raw(file_path):
    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def series_to_dataframe(
    values,
    value_column,
):
    df = pd.DataFrame(
        values,
        columns=[
            "market_timestamp",
            value_column,
        ],
    )

    df["market_timestamp"] = pd.to_datetime(
        df["market_timestamp"],
        unit="ms",
        utc=True,
    )

    return df


def transform_historical_data(payload):
    crypto_id = payload["crypto_id"]

    ingestion_timestamp = pd.to_datetime(
        payload["ingestion_timestamp"],
        format="%Y%m%dT%H%M%SZ",
        utc=True,
    )

    market_data = payload["data"]

    # Convert each CoinGecko array into a table

    prices_df = series_to_dataframe(
        market_data["prices"],
        "price_usd",
    )

    market_caps_df = series_to_dataframe(
        market_data["market_caps"],
        "market_cap_usd",
    )

    volumes_df = series_to_dataframe(
        market_data["total_volumes"],
        "total_volume_usd",
    )

    # Join everything using the market timestamp

    df = prices_df.merge(
        market_caps_df,
        on="market_timestamp",
        how="outer",
    )

    df = df.merge(
        volumes_df,
        on="market_timestamp",
        how="outer",
    )


    # Add pipeline metadata

    df["crypto_id"] = crypto_id

    df["ingestion_timestamp"] = (
        ingestion_timestamp
    )

    # Arrange columns
  

    df = df[
        [
            "crypto_id",
            "market_timestamp",
            "price_usd",
            "market_cap_usd",
            "total_volume_usd",
            "ingestion_timestamp",
        ]
    ]

    df = df.sort_values(
        "market_timestamp"
    ).reset_index(drop=True)

    return df


def validate_historical_data(df):
    if df.empty:
        raise ValueError(
            "Historical DataFrame is empty."
        )

    if df["crypto_id"].isna().any():
        raise ValueError(
            "crypto_id contains NULL values."
        )

    if df["market_timestamp"].isna().any():
        raise ValueError(
            "market_timestamp contains NULL values."
        )

    if df["price_usd"].isna().any():
        raise ValueError(
            "price_usd contains NULL values."
        )

    duplicate_count = df.duplicated(
        subset=[
            "crypto_id",
            "market_timestamp",
        ]
    ).sum()

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} "
            f"duplicate historical rows."
        )

    invalid_price_count = (
        df["price_usd"] <= 0
    ).sum()

    if invalid_price_count > 0:
        raise ValueError(
            f"Found {invalid_price_count} "
            f"non-positive historical prices."
        )

    print(
        "Historical data quality validation passed."
    )

    print(
        f"Validated {len(df)} historical rows."
    )


def save_historical_transformed(
    df,
    crypto_id,
):
    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_dir = (
        TRANSFORMED_DATA_DIR
        / crypto_id
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / f"{crypto_id}_history_{timestamp}.csv"
    )

    df.to_csv(
        output_file,
        index=False,
    )

    print(
        f"Historical transformed data saved locally: "
        f"{output_file}"
    )

    gcs_object_name = (
        f"transformed/historical/"
        f"{crypto_id}/"
        f"{timestamp[:4]}/"
        f"{timestamp[4:6]}/"
        f"{timestamp[6:8]}/"
        f"{crypto_id}_history_{timestamp}.csv"
    )

    gcs_uri = upload_file_to_gcs(
        output_file,
        gcs_object_name,
    )

    return gcs_uri


def process_historical_raw_object(
    raw_object_name,
):
    filename = Path(
        raw_object_name
    ).name

    crypto_id = (
        raw_object_name
        .split("/")[2]
    )

    local_dir = (
        RAW_DATA_DIR
        / crypto_id
    )

    local_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    local_raw_file = (
        local_dir
        / filename
    )

    download_file_from_gcs(
        raw_object_name,
        local_raw_file,
    )

    payload = load_historical_raw(
        local_raw_file
    )

    df = transform_historical_data(
        payload
    )

    print("\nHistorical transformed data:")
    print(df)

    print(
        "\nRunning historical data quality checks..."
    )

    validate_historical_data(df)

    transformed_gcs_uri = (
        save_historical_transformed(
            df,
            crypto_id,
        )
    )

    return transformed_gcs_uri


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--raw-object",
        required=True,
        help=(
            "GCS object name of the historical "
            "raw JSON file."
        ),
    )

    args = parser.parse_args()

    transformed_uri = (
        process_historical_raw_object(
            args.raw_object
        )
    )

    print(
        f"\nHistorical transformed GCS URI: "
        f"{transformed_uri}"
    )