from src.ingestion.coingecko import (
    get_top_crypto_ids,
)

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


TOP_N_CRYPTOCURRENCIES = 5
BACKFILL_DAYS = 30


def process_crypto(
    crypto_id,
    days,
):
    print(
        f"Processing historical data for: "
        f"{crypto_id}"
    )
    print("=" * 60)

    #Extraction

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

    #Raw GCS

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

    #Transformation & validation

    transformed_gcs_uri = (
        process_historical_raw_object(
            raw_object_name
        )
    )

    print(
        f"Transformed file: "
        f"{transformed_gcs_uri}"
    )

    # BigQuery MERGE

    load_historical_csv_from_gcs(
        transformed_gcs_uri,
        crypto_id
    )

    print(
        f"\nCompleted historical processing "
        f"for {crypto_id}."
    )


def run_backfill_pipeline():
    print("=" * 60)
    print("STARTING HISTORICAL BACKFILL PIPELINE")
    print("=" * 60)

    cryptocurrencies = get_top_crypto_ids(
        TOP_N_CRYPTOCURRENCIES
    )

    print(
        f"\nSelected cryptocurrencies: "
        f"{cryptocurrencies}"
    )

    for crypto_id in cryptocurrencies:
        process_crypto(
            crypto_id,
            BACKFILL_DAYS,
        )

    print("\n" + "=" * 60)
    print("BACKFILL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    run_backfill_pipeline()