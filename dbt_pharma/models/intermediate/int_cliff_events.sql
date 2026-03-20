-- int_cliff_events.sql
-- One row per drug cliff event: ticker, drug, NCE expiry date, cliff year.
-- Deduplicated by (ticker, trade_name) — earliest expiry date wins when
-- a drug has multiple NCE records (e.g., NCE and NCE-1 both present).

with drug_map as (
    select * from {{ ref('int_drug_company_map') }}
),

deduped as (
    select
        ticker,
        company_name,
        trade_name,
        ingredient,
        match_tier,
        min(exclusivity_date) as nce_expiry_date  -- earliest NCE expiry = binding cliff date
    from drug_map
    group by 1, 2, 3, 4, 5
),

final as (
    select
        ticker,
        company_name,
        trade_name,
        ingredient,
        match_tier,
        nce_expiry_date,
        year(nce_expiry_date) as cliff_year,
        -- Flag cliff proximity: how many years from today
        datediff('year', current_date(), nce_expiry_date) as years_to_cliff,
        -- Classify cliff timing
        case
            when datediff('year', current_date(), nce_expiry_date) < 0  then 'past'
            when datediff('year', current_date(), nce_expiry_date) <= 2 then 'imminent'
            when datediff('year', current_date(), nce_expiry_date) <= 5 then 'near_term'
            else 'long_term'
        end as cliff_category
    from deduped
)

select * from final
