import logging
from datetime import timedelta

from airflow.sdk import dag, task


TOP_N_CRYPTOCURRENCIES = 5
INCREMENTAL_DAYS = 2

logger = logging.getLogger(
    "airflow.task"
)


@dag(
    dag_id="crypto_incremental_pipeline",
    schedule=None,
    catchup=False,

    # Don't allow two complete pipeline runs
    # to overlap.
    max_active_runs=1,

    # Keep local resource/API usage modest.
    max_active_tasks=2,

    tags=[
        "crypto",
        "gcp",
        "incremental",
    ],
)
def crypto_incremental_pipeline():

    #Discover current top cryptocurrencies

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

        # Stable ordering makes Airflow's
        # mapped-task display easier to understand.
        cryptocurrencies = sorted(
            cryptocurrencies
        )

        logger.info(
            "Top cryptocurrencies: %s",
            cryptocurrencies,
        )

        return cryptocurrencies

    #Process each cryptocurrency

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

    #Final success marker

    @task
    def pipeline_complete():
        logger.info(
            "Crypto incremental pipeline "
            "completed successfully."
        )

    cryptocurrencies = (
        discover_top_coins()
    )

    processed_coins = (
        process_coin.expand(
            crypto_id=cryptocurrencies
        )
    )

    processed_coins >> pipeline_complete()


crypto_incremental_pipeline()