import json
from datetime import datetime, timezone
from pathlib import Path

import requests


API_URL = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
}


def fetch_crypto_data():
    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def save_raw_data(data):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"crypto_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Raw data saved to: {output_file}")


if __name__ == "__main__":
    data = fetch_crypto_data()

    print(f"Fetched {len(data)} cryptocurrencies")

    save_raw_data(data)