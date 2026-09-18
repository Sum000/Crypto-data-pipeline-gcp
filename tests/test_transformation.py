from datetime import datetime, timezone

from src.transformation.transformation import transform_data


def test_transform_data():
    raw_data = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_cap_rank": 1,
            "current_price": 65000.0,
            "market_cap": 1200000000000.0,
            "total_volume": 30000000000.0,
            "high_24h": 66000.0,
            "low_24h": 64000.0,
            "price_change_24h": 1000.0,
            "price_change_percentage_24h": 1.5,
            "circulating_supply": 19000000.0,
            "total_supply": 21000000.0,
            "max_supply": 21000000.0,
            "last_updated": "2026-09-18T05:00:00.000Z",
        }
    ]

    ingestion_timestamp = datetime(
        2026,
        9,
        18,
        5,
        10,
        tzinfo=timezone.utc,
    )

    df = transform_data(
        raw_data,
        ingestion_timestamp,
    )

    assert len(df) == 1

    assert df.loc[0, "crypto_id"] == "bitcoin"
    assert df.loc[0, "symbol"] == "btc"

    assert (
        df.loc[0, "current_price_usd"]
        == 65000.0
    )

    assert (
        df.loc[0, "market_cap_usd"]
        == 1200000000000.0
    )

    assert (
        df.loc[0, "ingestion_timestamp"]
        == ingestion_timestamp
    )