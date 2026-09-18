import time

from src.ingestion.coingecko import (
    get_top_crypto_ids,
)

from src.backfill_pipeline import (
    process_crypto,
)


TOP_N_CRYPTOCURRENCIES = 5

# Re-fetch a small overlapping window.
# BigQuery MERGE prevents duplicate records.
INCREMENTAL_DAYS = 2


def run_incremental_pipeline():
    print("STARTING DAILY INCREMENTAL PIPELINE")

    print(
        f"\nDiscovering top "
        f"{TOP_N_CRYPTOCURRENCIES} cryptocurrencies..."
    )

    cryptocurrencies = get_top_crypto_ids(
        TOP_N_CRYPTOCURRENCIES
    )

    print(
        f"Selected cryptocurrencies: "
        f"{cryptocurrencies}"
    )

    for crypto_id in cryptocurrencies:
        try:
            process_crypto(
                crypto_id,
                INCREMENTAL_DAYS,
            )

            # Small delay between API calls
            # to avoid hitting CoinGecko too aggressively.
            time.sleep(2)

        except Exception as error:
            print(
                f"\nIncremental processing failed "
                f"for {crypto_id}: {error}"
            )

            raise

    print(
        "DAILY INCREMENTAL PIPELINE "
        "COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    run_incremental_pipeline()