import pandas as pd
import pytest

from src.quality.validation import (
    validate_transformed_data,
)


def create_valid_dataframe():
    return pd.DataFrame(
        [
            {
                "crypto_id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "market_cap_rank": 1,
                "current_price_usd": 65000.0,
                "market_cap_usd": 1200000000000.0,
                "total_volume_usd": 30000000000.0,
                "high_24h_usd": 66000.0,
                "low_24h_usd": 64000.0,
                "price_change_24h_usd": 1000.0,
                "price_change_24h_pct": 1.5,
                "circulating_supply": 19000000.0,
                "total_supply": 21000000.0,
                "max_supply": 21000000.0,
                "last_updated": pd.Timestamp(
                    "2026-09-18T05:00:00Z"
                ),
                "ingestion_timestamp": pd.Timestamp(
                    "2026-09-18T05:10:00Z"
                ),
            }
        ]
    )


def test_valid_data_passes():
    df = create_valid_dataframe()

    validate_transformed_data(df)


def test_negative_price_fails():
    df = create_valid_dataframe()

    df.loc[
        0,
        "current_price_usd",
    ] = -100

    with pytest.raises(
        ValueError,
        match="non-positive prices",
    ):
        validate_transformed_data(df)


def test_duplicate_crypto_fails():
    df = create_valid_dataframe()

    df = pd.concat(
        [df, df],
        ignore_index=True,
    )

    with pytest.raises(
        ValueError,
        match="duplicate crypto_id",
    ):
        validate_transformed_data(df)