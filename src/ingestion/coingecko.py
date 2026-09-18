import json
from datetime import datetime, timezone
from src.storage.gcs import upload_file_to_gcs
from pathlib import Path

import requests


API_URL = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
}


def fetch_crypto_data(limit=10):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def get_top_crypto_ids(limit=5):
    market_data = fetch_crypto_data(
        limit=limit
    )

    crypto_ids = [
        coin["id"]
        for coin in market_data
    ]

    return crypto_ids


def save_raw_data(data):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    output_dir = PROJECT_ROOT / "data" / "raw"

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"crypto_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Raw data saved locally: {output_file}")

    gcs_object_name = (
        f"raw/crypto/"
        f"{timestamp[:4]}/"
        f"{timestamp[4:6]}/"
        f"{timestamp[6:8]}/"
        f"crypto_{timestamp}.json"
    )

    upload_file_to_gcs(
        output_file,
        gcs_object_name,
    )

    return gcs_object_name


if __name__ == "__main__":
    data = fetch_crypto_data()

    print(f"Fetched {len(data)} cryptocurrencies")

    save_raw_data(data)