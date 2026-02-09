-- Sample Athena Queries for Multi-Asset Portfolio Intelligence Pipeline
-- These queries demonstrate SQL proficiency across the pipeline's three data domains

-- ============================================================
-- 1. WINDOW FUNCTION: Daily price change leader per trading day
-- ============================================================
SELECT symbol, trading_day, change_percent
FROM (
    SELECT symbol, trading_day, change_percent,
           ROW_NUMBER() OVER (PARTITION BY trading_day ORDER BY change_percent DESC) AS rn
    FROM stocks
)
WHERE rn = 1
ORDER BY trading_day DESC;

-- ============================================================
-- 2. UNION: Cross-asset portfolio summary (stocks + crypto + weather)
-- ============================================================
SELECT 'Stocks' AS asset_class, COUNT(DISTINCT symbol) AS assets, ROUND(AVG(price), 2) AS avg_price
FROM stocks
UNION ALL
SELECT 'Crypto', COUNT(DISTINCT symbol), ROUND(AVG(price), 2)
FROM crypto
UNION ALL
SELECT 'Weather', 1, ROUND(AVG(temperature), 2)
FROM weather;

-- ============================================================
-- 3. AGGREGATION: Stock volatility ranking by daily change spread
-- ============================================================
SELECT symbol,
       COUNT(*) AS data_points,
       ROUND(MIN(change_percent), 2) AS worst_day,
       ROUND(MAX(change_percent), 2) AS best_day,
       ROUND(MAX(change_percent) - MIN(change_percent), 2) AS spread
FROM stocks
GROUP BY symbol
HAVING COUNT(*) > 1
ORDER BY spread DESC;

-- ============================================================
-- 4. WINDOW FUNCTION: Running average price per stock (moving avg)
-- ============================================================
SELECT symbol, trading_day, price,
       ROUND(AVG(price) OVER (PARTITION BY symbol ORDER BY trading_day
                               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3d
FROM stocks
ORDER BY symbol, trading_day;

-- ============================================================
-- 5. JOIN: Stocks with above-average volume days
-- ============================================================
SELECT s.symbol, s.trading_day, s.price, s.volume, s.change_percent,
       ROUND(avg_vol.avg_volume, 0) AS avg_volume,
       ROUND(CAST(s.volume AS DOUBLE) / avg_vol.avg_volume, 2) AS volume_ratio
FROM stocks s
JOIN (
    SELECT symbol, AVG(volume) AS avg_volume
    FROM stocks
    GROUP BY symbol
) avg_vol ON s.symbol = avg_vol.symbol
WHERE s.volume > avg_vol.avg_volume * 1.5
ORDER BY volume_ratio DESC;

-- ============================================================
-- 6. CRYPTO: Price comparison across collection times
-- ============================================================
SELECT symbol,
       MIN(price) AS low,
       MAX(price) AS high,
       ROUND(MAX(price) - MIN(price), 2) AS range,
       ROUND((MAX(price) - MIN(price)) / MIN(price) * 100, 2) AS range_pct
FROM crypto
GROUP BY symbol
ORDER BY range_pct DESC;

-- ============================================================
-- 7. DEDUP + CASE: AI Value Chain category performance
--    Handles duplicate records from multiple pipeline runs
-- ============================================================
SELECT
    CASE
        WHEN symbol = 'NVDA' THEN 'Infrastructure'
        WHEN symbol IN ('META', 'GOOGL') THEN 'AI Builder'
        WHEN symbol IN ('MSFT', 'AMZN') THEN 'AI Integrator'
        WHEN symbol IN ('CRM', 'ORCL', 'ADBE') THEN 'Legacy Tech'
        ELSE 'Control'
    END AS category,
    ROUND(AVG(change_percent), 2) AS avg_daily_change,
    ROUND(AVG(volume), 0) AS avg_volume,
    COUNT(DISTINCT symbol) AS stocks
FROM (
    SELECT symbol, trading_day, change_percent, volume,
           ROW_NUMBER() OVER (PARTITION BY symbol, trading_day ORDER BY extracted_at DESC) AS rn
    FROM stocks
)
WHERE rn = 1
GROUP BY CASE
    WHEN symbol = 'NVDA' THEN 'Infrastructure'
    WHEN symbol IN ('META', 'GOOGL') THEN 'AI Builder'
    WHEN symbol IN ('MSFT', 'AMZN') THEN 'AI Integrator'
    WHEN symbol IN ('CRM', 'ORCL', 'ADBE') THEN 'Legacy Tech'
    ELSE 'Control'
END
ORDER BY avg_daily_change DESC;

-- ============================================================
-- 8. DOLLAR VOLUME: Liquidity ranking (price x volume)
-- ============================================================
SELECT symbol, price, volume,
       ROUND(price * volume / 1000000000, 2) AS dollar_volume_B,
       change_percent
FROM (
    SELECT symbol, price, volume, change_percent,
           ROW_NUMBER() OVER (PARTITION BY symbol, trading_day ORDER BY extracted_at DESC) AS rn
    FROM stocks
)
WHERE rn = 1
ORDER BY price * volume DESC;
