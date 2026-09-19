from src.config import LOCAL_DATA_ROOT
import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.storage.gcs import upload_file_to_gcs

API_URL = (
    "https://api.coingecko.com/api/v3/"
    "coins/{crypto_id}/market_chart"
)

# PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_retry_session():
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    return session


SESSION = create_retry_session()

def fetch_historical_data(
    crypto_id,
    days=30,
):
    url = API_URL.format(
        crypto_id=crypto_id
    )

    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily",
    }

    print(
        f"Requesting historical data "
        f"for {crypto_id}..."
    )

    response = SESSION.get(
        url,
        params=params,

        # 10 seconds to establish connection,
        # 60 seconds to wait for response.
        timeout=(10, 60),
    )

    response.raise_for_status()

    return response.json()


def save_historical_raw_data(
    crypto_id,
    data,
    days,
):
    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    # Add metadata around the raw API response

    raw_payload = {
        "crypto_id": crypto_id,
        "vs_currency": "usd",
        "requested_days": days,
        "ingestion_timestamp": timestamp,
        "data": data,
    }

    # Save local raw copy

    output_dir = (
    LOCAL_DATA_ROOT
    / "raw"
    / "historical"
    / crypto_id
)
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / f"{crypto_id}_history_{timestamp}.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            raw_payload,
            file,
            indent=2,
        )

    print(
        f"Historical raw data saved locally: "
        f"{output_file}"
    )

    # Upload raw historical file to GCS

    gcs_object_name = (
        f"raw/historical/"
        f"{crypto_id}/"
        f"{timestamp[:4]}/"
        f"{timestamp[4:6]}/"
        f"{timestamp[6:8]}/"
        f"{crypto_id}_history_{timestamp}.json"
    )

    gcs_uri = upload_file_to_gcs(
        output_file,
        gcs_object_name,
    )

    return gcs_object_name, gcs_uri


if __name__ == "__main__":
    crypto_id = "bitcoin"
    days = 30

    print(
        f"Fetching {days} days of "
        f"historical data for {crypto_id}..."
    )

    historical_data = fetch_historical_data(
        crypto_id,
        days,
    )

    print(
        f"Price points received: "
        f"{len(historical_data['prices'])}"
    )

    raw_object_name, gcs_uri = (
        save_historical_raw_data(
            crypto_id,
            historical_data,
            days,
        )
    )

    print(
        f"Historical raw GCS object: "
        f"{raw_object_name}"
    )

    print(
        f"Historical raw GCS URI: "
        f"{gcs_uri}"
    )