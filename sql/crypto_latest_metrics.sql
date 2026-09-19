CREATE OR REPLACE VIEW
  `project-bbbd1cf0-de1e-476f-af1.crypto_data.crypto_latest_metrics`
AS

SELECT
    crypto_id,
    market_date,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    daily_return_pct,
    price_7d_moving_avg,
    volume_7d_moving_avg,
    volatility_7d

FROM
    `project-bbbd1cf0-de1e-476f-af1.crypto_data.crypto_daily_metrics`

QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY crypto_id
        ORDER BY market_date DESC
    ) = 1;