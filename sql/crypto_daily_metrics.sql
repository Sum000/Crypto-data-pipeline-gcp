CREATE OR REPLACE VIEW
  `project-bbbd1cf0-de1e-476f-af1.crypto_data.crypto_daily_metrics`
AS

WITH daily_data AS (
    SELECT
        crypto_id,
        DATE(market_timestamp) AS market_date,

        ARRAY_AGG(
            price_usd
            ORDER BY market_timestamp DESC
            LIMIT 1
        )[OFFSET(0)] AS price_usd,

        ARRAY_AGG(
            market_cap_usd
            ORDER BY market_timestamp DESC
            LIMIT 1
        )[OFFSET(0)] AS market_cap_usd,

        ARRAY_AGG(
            total_volume_usd
            ORDER BY market_timestamp DESC
            LIMIT 1
        )[OFFSET(0)] AS total_volume_usd

    FROM
        `project-bbbd1cf0-de1e-476f-af1.crypto_data.crypto_daily_history`

    GROUP BY
        crypto_id,
        market_date
),

with_previous_price AS (
    SELECT
        *,
        LAG(price_usd) OVER (
            PARTITION BY crypto_id
            ORDER BY market_date
        ) AS previous_price_usd

    FROM daily_data
),

with_returns AS (
    SELECT
        *,

        SAFE_DIVIDE(
            price_usd - previous_price_usd,
            previous_price_usd
        ) * 100 AS daily_return_pct

    FROM with_previous_price
)

SELECT
    crypto_id,
    market_date,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    previous_price_usd,
    daily_return_pct,

    AVG(price_usd) OVER (
        PARTITION BY crypto_id
        ORDER BY market_date
        ROWS BETWEEN 6 PRECEDING
        AND CURRENT ROW
    ) AS price_7d_moving_avg,

    AVG(total_volume_usd) OVER (
        PARTITION BY crypto_id
        ORDER BY market_date
        ROWS BETWEEN 6 PRECEDING
        AND CURRENT ROW
    ) AS volume_7d_moving_avg,

    STDDEV_SAMP(daily_return_pct) OVER (
        PARTITION BY crypto_id
        ORDER BY market_date
        ROWS BETWEEN 6 PRECEDING
        AND CURRENT ROW
    ) AS volatility_7d

FROM with_returns;