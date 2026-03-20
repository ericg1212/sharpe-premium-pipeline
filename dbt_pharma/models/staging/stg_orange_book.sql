-- stg_orange_book.sql
-- Clean and type-cast Orange Book NCE exclusivity records.
-- Source: RAW.ORANGE_BOOK (loaded via COPY INTO from S3 Parquet)
--
-- Column note: RAW table has trade_name + ingredient + applicant_full_name
-- (joined from products.txt at extract time). Original exclusivity.txt only
-- had 5 cols — drug name required the join.
--
-- Why NCE only: NCE = New Chemical Entity 5-year exclusivity. This is the
-- binding constraint for small-molecule generic entry — not patent expiry.

with source as (
    select * from {{ source('raw', 'orange_book') }}
),

nce_only as (
    select
        appl_type,
        appl_no,
        product_no,
        upper(trim(trade_name))          as trade_name,
        upper(trim(ingredient))          as ingredient,
        trim(applicant_full_name)        as applicant_full_name,
        upper(trim(exclusivity_code))    as exclusivity_code,
        try_cast(exclusivity_date as date) as exclusivity_date,
        _loaded_at
    from source
    where exclusivity_code in ('NCE', 'NCE-1')
      and trade_name is not null
      and exclusivity_date is not null
)

select * from nce_only
