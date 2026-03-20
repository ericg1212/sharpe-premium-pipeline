-- mart_drawdown_timeline.sql
-- Final output for Page 2 (Drawdown Analysis).
-- One row per drug cliff event with full drawdown and recovery metrics.

with drawdown as (
    select * from {{ ref('int_stock_drawdown') }}
),

cliff as (
    select ticker, trade_name, nce_expiry_date, company_name, cliff_year
    from {{ ref('int_cliff_events') }}
),

final as (
    select
        d.ticker,
        c.company_name,
        d.trade_name,
        d.nce_expiry_date   as cliff_date,
        c.cliff_year,
        d.peak_price,
        d.trough_price,
        d.drawdown_pct,
        d.drawdown_duration_days,
        d.recovery_date,
        d.recovery_days,
        d.market_cap_pre,
        d.market_cap_post,
        -- Market cap at risk (absolute $)
        d.market_cap_pre - d.market_cap_post as market_cap_at_risk
    from drawdown d
    inner join cliff c
        on c.ticker = d.ticker
        and c.trade_name = d.trade_name
        and c.nce_expiry_date = d.nce_expiry_date
)

select * from final
order by cliff_date asc, ticker
