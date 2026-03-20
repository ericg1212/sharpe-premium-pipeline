-- stg_edgar.sql
-- Clean and type-cast SEC EDGAR annual revenue + R&D expense data.
-- Source: RAW.EDGAR (loaded via COPY INTO from S3 Parquet)
--
-- Key schema notes:
--   concept  — XBRL label (varies by company; see config.py EDGAR_REVENUE_CONCEPTS)
--   metric   — 'revenue' or 'rd_expense' (assigned at extract time)
--   form     — '10-K' (us-gaap filers: MRK/BMY/LLY/PFE/JNJ/ABBV)
--              '20-F' (IFRS filers: AZN uses ifrs-full/Revenue,
--                                   NVS uses ifrs-full/RevenueFromSaleOfGoods)
--   value_usd — always in USD (NVS reports USD on 20-F filings)
--
-- Downstream: use COALESCE across concepts ordered by end_date recency
-- in int/mart models — do not pick a single concept here.

with source as (
    select * from {{ source('raw', 'edgar') }}
),

cleaned as (
    select
        upper(trim(ticker))          as ticker,
        trim(concept)                as concept,
        lower(trim(metric))          as metric,
        upper(trim(form))            as form,
        try_cast(end_date as date)   as end_date,
        try_cast(value_usd as bigint) as value_usd,
        try_cast(filed as date)      as filed,
        trim(accn)                   as accn,
        _loaded_at
    from source
    where ticker is not null
      and end_date is not null
      and value_usd is not null
      and value_usd > 0              -- exclude zero/negative (restatement artifacts)
      and metric in ('revenue', 'rd_expense')
)

select * from cleaned
