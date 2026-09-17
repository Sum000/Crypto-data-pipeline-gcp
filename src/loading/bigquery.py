from google.api_core.exceptions import NotFound
from google.cloud import bigquery


PROJECT_ID = "project-bbbd1cf0-de1e-476f-af1"
DATASET_ID = "crypto_data"

FINAL_TABLE_ID = "crypto_market_snapshot_v2"
STAGING_TABLE_ID = "crypto_market_staging"

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
            "Created BigQuery staging table."
        )


def load_csv_to_staging(
    client,
    gcs_uri,
):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,

        # Replace staging contents every run.
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    print(
        f"Loading {gcs_uri} "
        f"into staging table..."
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
        f"Loaded {staging_table.num_rows} "
        f"rows into staging."
    )


def merge_staging_to_final(client):
    merge_query = f"""
    MERGE `{FINAL_TABLE_REFERENCE}` AS target

    USING `{STAGING_TABLE_REFERENCE}` AS source

    ON
        target.crypto_id = source.crypto_id
        AND target.ingestion_timestamp =
            source.ingestion_timestamp

    WHEN NOT MATCHED THEN

    INSERT (
        crypto_id,
        symbol,
        name,
        market_cap_rank,
        current_price_usd,
        market_cap_usd,
        total_volume_usd,
        high_24h_usd,
        low_24h_usd,
        price_change_24h_usd,
        price_change_24h_pct,
        circulating_supply,
        total_supply,
        max_supply,
        last_updated,
        ingestion_timestamp
    )

    VALUES (
        source.crypto_id,
        source.symbol,
        source.name,
        source.market_cap_rank,
        source.current_price_usd,
        source.market_cap_usd,
        source.total_volume_usd,
        source.high_24h_usd,
        source.low_24h_usd,
        source.price_change_24h_usd,
        source.price_change_24h_pct,
        source.circulating_supply,
        source.total_supply,
        source.max_supply,
        source.last_updated,
        source.ingestion_timestamp
    )
    """

    print(
        "Merging staging data "
        "into final table..."
    )

    query_job = client.query(
        merge_query
    )

    query_job.result()

    print(
        "BigQuery MERGE completed."
    )


def load_csv_from_gcs(gcs_uri):
    client = bigquery.Client(
        project=PROJECT_ID
    )

    ensure_staging_table(client)

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
        "BigQuery load completed successfully."
    )

    print(
        f"Final table row count: "
        f"{final_table.num_rows}"
    )


if __name__ == "__main__":
    print(
        "BigQuery loading module. "
        "Run the complete pipeline using "
        "`python -m src.pipeline`."
    )