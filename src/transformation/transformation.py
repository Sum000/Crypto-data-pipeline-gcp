import json
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
TRANSFORMED_DATA_DIR = PROJECT_ROOT / "data" / "transformed"


def get_latest_raw_file():
    files = sorted(RAW_DATA_DIR.glob("crypto_*.json"))

    if not files:
        raise FileNotFoundError("No raw crypto data found.")

    return files[-1]


def load_raw_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def transform_data(data):
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

    df["ingestion_timestamp"] = pd.Timestamp.now(tz="UTC")

    return df


def save_transformed_data(df):
    TRANSFORMED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    output_file = TRANSFORMED_DATA_DIR / "crypto_market.csv"

    df.to_csv(output_file, index=False)

    print(f"Transformed data saved to: {output_file}")


if __name__ == "__main__":
    raw_file = get_latest_raw_file()

    print(f"Reading: {raw_file}")

    data = load_raw_data(raw_file)

    df = transform_data(data)

    print("\nTransformed data:")
    print(df)

    save_transformed_data(df)