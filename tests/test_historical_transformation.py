from src.transformation.historical import (
    transform_historical_data,
    validate_historical_data,
)


def test_historical_transformation():
    payload = {
        "crypto_id": "bitcoin",
        "vs_currency": "usd",
        "requested_days": 2,
        "ingestion_timestamp":
            "20260918T050000Z",
        "data": {
            "prices": [
                [
                    1789700000000,
                    65000.0,
                ],
                [
                    1789786400000,
                    66000.0,
                ],
            ],
            "market_caps": [
                [
                    1789700000000,
                    1200000000000.0,
                ],
                [
                    1789786400000,
                    1210000000000.0,
                ],
            ],
            "total_volumes": [
                [
                    1789700000000,
                    30000000000.0,
                ],
                [
                    1789786400000,
                    31000000000.0,
                ],
            ],
        },
    }

    df = transform_historical_data(
        payload
    )

    assert len(df) == 2

    assert (
        df.loc[0, "crypto_id"]
        == "bitcoin"
    )

    assert (
        df.loc[0, "price_usd"]
        == 65000.0
    )

    assert (
        df.loc[1, "price_usd"]
        == 66000.0
    )

    validate_historical_data(df)