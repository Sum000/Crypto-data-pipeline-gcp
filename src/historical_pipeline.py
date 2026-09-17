from src.ingestion.historical import (
    fetch_historical_data,
    save_historical_raw_data,
)

from src.transformation.historical import (
    process_historical_raw_object,
)

from src.loading.historical_bigquery import (
    load_historical_csv_from_gcs,
)


CRYPTOCURRENCIES = [
    "bitcoin",
    "ethereum",
    "solana",
    "ripple",
    "binancecoin",
]

HISTORICAL_DAYS = 30


def process_crypto(
    crypto_id,
    days,
):
    print(
        f"Processing historical data for: "
        f"{crypto_id}"
    )

    # Extracting historical data

    print(
        f"\nFetching {days} days "
        f"of historical data..."
    )

    data = fetch_historical_data(
        crypto_id,
        days,
    )

    print(
        f"Received {len(data['prices'])} "
        f"price observations."
    )

    # Storing raw JSON in GCS

    raw_object_name, raw_gcs_uri = (
        save_historical_raw_data(
            crypto_id,
            data,
            days,
        )
    )

    print(
        f"Raw object: "
        f"{raw_object_name}"
    )

    # STEP 3: Transform + validate

    transformed_gcs_uri = (
        process_historical_raw_object(
            raw_object_name
        )
    )

    print(
        f"Transformed file: "
        f"{transformed_gcs_uri}"
    )

    # Load into BigQuery

    load_historical_csv_from_gcs(
        transformed_gcs_uri
    )

    print(
        f"\nCompleted historical pipeline "
        f"for {crypto_id}."
    )


def run_historical_pipeline():
    print("STARTING HISTORICAL CRYPTO PIPELINE")

    for crypto_id in CRYPTOCURRENCIES:
        try:
            process_crypto(
                crypto_id,
                HISTORICAL_DAYS,
            )

        except Exception as error:
            print(
                f"\nPipeline failed for "
                f"{crypto_id}: {error}"
            )

            raise

    print(
        "HISTORICAL PIPELINE "
        "COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    run_historical_pipeline()