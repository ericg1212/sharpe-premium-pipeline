-- int_stock_drawdown.sql
-- For each cliff event, calculate peak-to-trough price decline and recovery.
-- Window: 2 years before cliff date to 3 years after.
--
-- Drawdown methodology:
--   peak = max close in the 2yr window before cliff date
--   trough = min close in the 12 months after cliff date
--   drawdown_pct = (trough - peak) / peak * 100  (negative = decline)
--   recovery_date = first date after trough where close >= peak again
--   drawdown_duration_days = days from cliff date to trough date

with cliff as (
    select * from {{ ref('int_cliff_events') }}
),

prices as (
    select * from {{ ref('stg_yfinance') }}
),

-- Pre-cliff peak: highest close in 2yr window before NCE expiry
pre_cliff_peak as (
    select
        c.ticker,
        c.trade_name,
        c.nce_expiry_date,
        max(p.close) as peak_price,
        max(p.market_cap) as market_cap_pre
    from cliff c
    inner join prices p
        on p.ticker = c.ticker
        and p.date between dateadd('year', -2, c.nce_expiry_date)
                       and c.nce_expiry_date
    group by 1, 2, 3
),

-- Post-cliff trough: lowest close in 12 months after NCE expiry
post_cliff_trough as (
    select
        c.ticker,
        c.trade_name,
        c.nce_expiry_date,
        min(p.close) as trough_price,
        min(p.market_cap) as market_cap_post,
        -- Date of trough (for duration calc)
        min_by(p.date, p.close) as trough_date
    from cliff c
    inner join prices p
        on p.ticker = c.ticker
        and p.date between c.nce_expiry_date
                       and dateadd('month', 12, c.nce_expiry_date)
    group by 1, 2, 3
),

combined as (
    select
        pk.ticker,
        pk.trade_name,
        pk.nce_expiry_date,
        pk.peak_price,
        pk.market_cap_pre,
        tr.trough_price,
        tr.market_cap_post,
        tr.trough_date,
        -- Drawdown % (negative)
        round(
            (tr.trough_price - pk.peak_price) / nullif(pk.peak_price, 0) * 100,
            2
        ) as drawdown_pct,
        -- Days from cliff to trough
        datediff('day', pk.nce_expiry_date, tr.trough_date) as drawdown_duration_days
    from pre_cliff_peak pk
    inner join post_cliff_trough tr
        on tr.ticker = pk.ticker
        and tr.trade_name = pk.trade_name
        and tr.nce_expiry_date = pk.nce_expiry_date
),

-- Recovery: first date after trough where close >= peak_price
recovery as (
    select
        c2.ticker,
        c2.trade_name,
        c2.nce_expiry_date,
        min(p2.date) as recovery_date
    from combined c2
    inner join prices p2
        on p2.ticker = c2.ticker
        and p2.date > c2.trough_date
        and p2.close >= c2.peak_price
    group by 1, 2, 3
),

final as (
    select
        c.ticker,
        c.trade_name,
        c.nce_expiry_date,
        c.peak_price,
        c.trough_price,
        c.drawdown_pct,
        c.drawdown_duration_days,
        c.market_cap_pre,
        c.market_cap_post,
        r.recovery_date,
        datediff('day', c.nce_expiry_date, r.recovery_date) as recovery_days
    from combined c
    left join recovery r
        on r.ticker = c.ticker
        and r.trade_name = c.trade_name
        and r.nce_expiry_date = c.nce_expiry_date
)

select * from final
