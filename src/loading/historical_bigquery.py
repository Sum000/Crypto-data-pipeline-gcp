import argparse

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


PROJECT_ID = "project-bbbd1cf0-de1e-476f-af1"
DATASET_ID = "crypto_data"

FINAL_TABLE_ID = "crypto_daily_history"
STAGING_TABLE_ID = "crypto_daily_history_staging"

FINAL_TABLE_REFERENCE = (
    f"{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE_ID}"
)

STAGING_TABLE_REFERENCE = (
    f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"
)


def ensure_staging_table(client):
    try:
        client.get_table(
            STAGING_TABLE_REFERENCE
        )

    except NotFound:
        final_table = client.get_table(
            FINAL_TABLE_REFERENCE
        )

        staging_table = bigquery.Table(
            STAGING_TABLE_REFERENCE,
            schema=final_table.schema,
        )

        client.create_table(
            staging_table
        )

        print(
            "Created historical staging table."
        )


def load_csv_to_staging(
    client,
    gcs_uri,
):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    print(
        f"Loading historical file into staging:\n"
        f"{gcs_uri}"
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        STAGING_TABLE_REFERENCE,
        job_config=job_config,
    )

    load_job.result()

    staging_table = client.get_table(
        STAGING_TABLE_REFERENCE
    )

    print(
        f"Historical staging rows: "
        f"{staging_table.num_rows}"
    )


def merge_staging_to_final(client):
    merge_query = f"""
    MERGE `{FINAL_TABLE_REFERENCE}` AS target

    USING `{STAGING_TABLE_REFERENCE}` AS source

    ON
        target.crypto_id = source.crypto_id
        AND target.market_timestamp =
            source.market_timestamp

    WHEN MATCHED THEN

      UPDATE SET

        target.price_usd =
            source.price_usd,

        target.market_cap_usd =
            source.market_cap_usd,

        target.total_volume_usd =
            source.total_volume_usd,

        target.ingestion_timestamp =
            source.ingestion_timestamp

    WHEN NOT MATCHED THEN

      INSERT (
        crypto_id,
        market_timestamp,
        price_usd,
        market_cap_usd,
        total_volume_usd,
        ingestion_timestamp
      )

      VALUES (
        source.crypto_id,
        source.market_timestamp,
        source.price_usd,
        source.market_cap_usd,
        source.total_volume_usd,
        source.ingestion_timestamp
      )
    """

    print(
        "Merging historical staging data "
        "into final table..."
    )

    query_job = client.query(
        merge_query
    )

    query_job.result()

    print(
        "Historical BigQuery MERGE completed."
    )


def load_historical_csv_from_gcs(
    gcs_uri,
):
    client = bigquery.Client(
        project=PROJECT_ID
    )

    ensure_staging_table(
        client
    )

    load_csv_to_staging(
        client,
        gcs_uri,
    )

    merge_staging_to_final(
        client
    )

    final_table = client.get_table(
        FINAL_TABLE_REFERENCE
    )

    print(
        "Historical BigQuery load "
        "completed successfully."
    )

    print(
        f"Historical table row count: "
        f"{final_table.num_rows}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--gcs-uri",
        required=True,
        help=(
            "GCS URI of the transformed "
            "historical CSV."
        ),
    )

    args = parser.parse_args()

    load_historical_csv_from_gcs(
        args.gcs_uri
    )