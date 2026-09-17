import pandas as pd


REQUIRED_COLUMNS = [
    "crypto_id",
    "symbol",
    "name",
    "market_cap_rank",
    "current_price_usd",
    "market_cap_usd",
    "total_volume_usd",
    "high_24h_usd",
    "low_24h_usd",
    "price_change_24h_usd",
    "price_change_24h_pct",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "last_updated",
    "ingestion_timestamp",
]


def validate_transformed_data(df: pd.DataFrame):
    errors = []

    # 1. Dataset must not be empty
    if df.empty:
        errors.append(
            "DataFrame is empty."
        )

    # 2. Required columns must exist
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        errors.append(
            f"Missing required columns: {missing_columns}"
        )

    
    if missing_columns:
        raise ValueError(
            "Data quality validation failed:\n"
            + "\n".join(errors)
        )

    # 3. Critical fields cannot be NULL
    critical_columns = [
        "crypto_id",
        "symbol",
        "name",
        "current_price_usd",
        "ingestion_timestamp",
    ]

    for column in critical_columns:
        null_count = df[column].isna().sum()

        if null_count > 0:
            errors.append(
                f"{column} contains "
                f"{null_count} NULL values."
            )

    # 4. crypto_id should be unique within one snapshot
    duplicate_count = df.duplicated(
        subset=["crypto_id"]
    ).sum()

    if duplicate_count > 0:
        errors.append(
            f"Found {duplicate_count} duplicate crypto_id values."
        )

    # 5. Prices should be positive
    invalid_price_count = (
        df["current_price_usd"] <= 0
    ).sum()

    if invalid_price_count > 0:
        errors.append(
            f"Found {invalid_price_count} "
            f"non-positive prices."
        )

    # 6. Market cap cannot be negative
    invalid_market_cap_count = (
        df["market_cap_usd"] < 0
    ).sum()

    if invalid_market_cap_count > 0:
        errors.append(
            f"Found {invalid_market_cap_count} "
            f"negative market cap values."
        )

    # 7. Trading volume cannot be negative
    invalid_volume_count = (
        df["total_volume_usd"] < 0
    ).sum()

    if invalid_volume_count > 0:
        errors.append(
            f"Found {invalid_volume_count} "
            f"negative volume values."
        )

    # 8. 24h high should not be lower than 24h low
    invalid_high_low = (
        df["high_24h_usd"]
        < df["low_24h_usd"]
    ).sum()

    if invalid_high_low > 0:
        errors.append(
            f"Found {invalid_high_low} rows where "
            f"high_24h_usd < low_24h_usd."
        )

    # Final validation result
    if errors:
        raise ValueError(
            "Data quality validation failed:\n- "
            + "\n- ".join(errors)
        )

    print(
        "Data quality validation passed."
    )
    print(
        f"Validated {len(df)} rows."
    )