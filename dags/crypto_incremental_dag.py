import logging
from datetime import datetime, timedelta, timezone

import pendulum

from airflow.sdk import dag, task


TOP_N_CRYPTOCURRENCIES = 5
INCREMENTAL_DAYS = 2

PROJECT_ID = "project-bbbd1cf0-de1e-476f-af1"

HISTORY_TABLE = (
    "project-bbbd1cf0-de1e-476f-af1."
    "crypto_data.crypto_daily_history"
)

logger = logging.getLogger(
    "airflow.task"
)


@dag(
    dag_id="crypto_incremental_pipeline",

    # Run every day at 08:00 India time.
    schedule="0 8 * * *",

    start_date=pendulum.datetime(
        2026,
        9,
        18,
        tz="Asia/Kolkata",
    ),

    catchup=False,

    # Don't allow two complete DAG runs
    # to overlap.
    max_active_runs=1,

    # Keep RAM/API usage modest on the
    # local 8 GB laptop.
    max_active_tasks=2,

    tags=[
        "crypto",
        "gcp",
        "incremental",
    ],
)
def crypto_incremental_pipeline():

    # Discover current top cryptocurrencies

    @task(
        retries=2,
        retry_delay=timedelta(
            minutes=1
        ),
    )
    def discover_top_coins():

        from src.ingestion.coingecko import (
            get_top_crypto_ids,
        )

        cryptocurrencies = (
            get_top_crypto_ids(
                TOP_N_CRYPTOCURRENCIES
            )
        )

        cryptocurrencies = sorted(
            cryptocurrencies
        )

        logger.info(
            "Top cryptocurrencies selected: %s",
            cryptocurrencies,
        )

        return cryptocurrencies

    # Process each cryptocurrency
    @task(
        retries=2,
        retry_delay=timedelta(
            minutes=2
        ),
    )
    def process_coin(
        crypto_id,
    ):

        from src.backfill_pipeline import (
            process_crypto,
        )

        logger.info(
            "Starting incremental processing "
            "for %s",
            crypto_id,
        )

        process_crypto(
            crypto_id,
            INCREMENTAL_DAYS,
        )

        logger.info(
            "Completed incremental processing "
            "for %s",
            crypto_id,
        )

        return crypto_id

    # Validate warehouse after all mapped tasks

    @task(
        retries=1,
        retry_delay=timedelta(
            minutes=1
        ),
    )
    def validate_warehouse(
        crypto_ids,
    ):

        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT_ID
        )

        # Duplicate natural keys

        duplicate_query = f"""
        SELECT
            crypto_id,
            market_timestamp,
            COUNT(*) AS row_count
        FROM
            `{HISTORY_TABLE}`
        WHERE
            crypto_id IN UNNEST(@crypto_ids)
        GROUP BY
            crypto_id,
            market_timestamp
        HAVING
            COUNT(*) > 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "crypto_ids",
                    "STRING",
                    crypto_ids,
                )
            ]
        )

        duplicate_rows = list(
            client.query(
                duplicate_query,
                job_config=job_config,
            ).result()
        )

        if duplicate_rows:
            raise ValueError(
                "Warehouse validation failed: "
                f"{len(duplicate_rows)} "
                "duplicate key groups found."
            )

        # Each selected cryptocurrency exists
        # and has recent data.


        freshness_query = f"""
        SELECT
            crypto_id,
            MAX(market_timestamp)
                AS latest_timestamp
        FROM
            `{HISTORY_TABLE}`
        WHERE
            crypto_id IN UNNEST(@crypto_ids)
        GROUP BY
            crypto_id
        """

        freshness_rows = list(
            client.query(
                freshness_query,
                job_config=job_config,
            ).result()
        )

        latest_by_crypto = {
            row.crypto_id:
                row.latest_timestamp
            for row in freshness_rows
        }

        missing_cryptos = (
            set(crypto_ids)
            - set(latest_by_crypto)
        )

        if missing_cryptos:
            raise ValueError(
                "Warehouse validation failed. "
                "Missing cryptocurrencies: "
                f"{sorted(missing_cryptos)}"
            )

        freshness_threshold = (
            datetime.now(
                timezone.utc
            )
            - timedelta(days=3)
        )

        stale_cryptos = []

        for (
            crypto_id,
            latest_timestamp,
        ) in latest_by_crypto.items():

            if (
                latest_timestamp
                < freshness_threshold
            ):
                stale_cryptos.append(
                    crypto_id
                )

        if stale_cryptos:
            raise ValueError(
                "Warehouse validation failed. "
                "Stale cryptocurrencies: "
                f"{sorted(stale_cryptos)}"
            )

        logger.info(
            "Warehouse quality validation passed."
        )

        logger.info(
            "Validated cryptocurrencies: %s",
            crypto_ids,
        )

    # Final completion marker

    @task
    def pipeline_complete():

        logger.info(
            "Crypto incremental pipeline "
            "completed successfully."
        )

    # DAG dependencies
    cryptocurrencies = (
        discover_top_coins()
    )

    processed_coins = (
        process_coin.expand(
            crypto_id=cryptocurrencies
        )
    )

    quality_check = (
        validate_warehouse(
            cryptocurrencies
        )
    )

    processed_coins >> quality_check

    quality_check >> pipeline_complete()


crypto_incremental_pipeline()