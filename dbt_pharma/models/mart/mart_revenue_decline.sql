-- mart_revenue_decline.sql
-- Final output for Page 1 (Revenue Cliff).
-- One row per drug cliff event with market cap pre/post and % decline.
--
-- Market cap % decline is the revenue proxy. v2 will replace with
-- actual revenue from 10-K EDGAR filings.

with cliff as (
    select * from {{ ref('int_cliff_events') }}
),

drawdown as (
    select * from {{ ref('int_stock_drawdown') }}
),

final as (
    select
        c.ticker,
        c.company_name,
        c.trade_name,
        c.ingredient,
        c.nce_expiry_date,
        c.cliff_year,
        c.cliff_category,
        c.years_to_cliff,
        d.market_cap_pre,
        d.market_cap_post,
        round(
            (d.market_cap_post - d.market_cap_pre) / nullif(d.market_cap_pre, 0) * 100,
            2
        ) as pct_decline,
        d.peak_price,
        d.trough_price,
        d.drawdown_pct as price_drawdown_pct
    from cliff c
    left join drawdown d
        on d.ticker = c.ticker
        and d.trade_name = c.trade_name
        and d.nce_expiry_date = c.nce_expiry_date
)

select * from final
order by cliff_year asc, ticker
