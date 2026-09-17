from src.ingestion.coingecko import (
    fetch_crypto_data,
    save_raw_data,
)

from src.transformation.transformation import (
    process_raw_object,
)

from src.loading.bigquery import (
    load_csv_from_gcs,
)


def run_pipeline():
    print("STARTING CRYPTO DATA PIPELINE")

    print("1.Extracting data from CoinGecko...")

    data = fetch_crypto_data()

    print(
        f"Fetched {len(data)} cryptocurrencies."
    )


    print("2.Saving raw data and transforming...")

    raw_object_name = save_raw_data(data)

    print(
        f"Raw GCS object: "
        f"{raw_object_name}"
    )

    transformed_gcs_uri = process_raw_object(
        raw_object_name
    )

    print(
        f"Transformed GCS file: "
        f"{transformed_gcs_uri}"
    )


    print("3.Loading data into BigQuery...")

    load_csv_from_gcs(
        transformed_gcs_uri
    )

    print("PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_pipeline()