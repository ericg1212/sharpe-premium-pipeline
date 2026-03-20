-- int_drug_company_map.sql
-- Join Orange Book drugs to ticker via two-tier lookup:
--   1. trade_name match (hardcoded for known drugs — high precision)
--   2. applicant_full_name match (fuzzy fallback — catches unmapped drugs)
-- COALESCE(tier1, tier2) — tier1 wins if both match.
--
-- Design rationale: pure NLP entity resolution adds zero value for 8 known
-- companies. The hardcoded lookup is transparent and auditable. Applicant
-- name fallback (available since products.txt join at extract time) improves
-- coverage without sacrificing auditability. A production version would add
-- an NLP layer on top for novel entrants.

with ob as (
    select * from {{ ref('stg_orange_book') }}
),

company_lookup as (
    select * from (values
        ('MRK',  'Merck'),
        ('BMY',  'Bristol-Myers Squibb'),
        ('AZN',  'AstraZeneca'),
        ('LLY',  'Eli Lilly'),
        ('PFE',  'Pfizer'),
        ('JNJ',  'Johnson & Johnson'),
        ('ABBV', 'AbbVie'),
        ('NVS',  'Novartis')
    ) as t(ticker, company_name)
),

mapped as (
    select
        ob.trade_name,
        ob.ingredient,
        ob.applicant_full_name,
        ob.appl_no,
        ob.exclusivity_code,
        ob.exclusivity_date,

        -- Tier 1: trade name match (high precision, known drugs)
        coalesce(
            case
                when ob.trade_name ilike '%keytruda%'    then 'MRK'
                when ob.trade_name ilike '%januvia%'     then 'MRK'
                when ob.trade_name ilike '%opdivo%'      then 'BMY'
                when ob.trade_name ilike '%eliquis%'     then 'BMY'
                when ob.trade_name ilike '%tagrisso%'    then 'AZN'
                when ob.trade_name ilike '%farxiga%'     then 'AZN'
                when ob.trade_name ilike '%mounjaro%'    then 'LLY'
                when ob.trade_name ilike '%trulicity%'   then 'LLY'
                when ob.trade_name ilike '%paxlovid%'    then 'PFE'
                when ob.trade_name ilike '%xeljanz%'     then 'PFE'
                when ob.trade_name ilike '%stelara%'     then 'JNJ'
                when ob.trade_name ilike '%darzalex%'    then 'JNJ'
                when ob.trade_name ilike '%humira%'      then 'ABBV'
                when ob.trade_name ilike '%skyrizi%'     then 'ABBV'
                when ob.trade_name ilike '%rinvoq%'      then 'ABBV'
                when ob.trade_name ilike '%entresto%'    then 'NVS'
                when ob.trade_name ilike '%kisqali%'     then 'NVS'
            end,
            -- Tier 2: applicant name fallback (catches drugs not yet hardcoded)
            case
                when ob.applicant_full_name ilike '%merck%'          then 'MRK'
                when ob.applicant_full_name ilike '%bristol%'        then 'BMY'
                when ob.applicant_full_name ilike '%astrazeneca%'    then 'AZN'
                when ob.applicant_full_name ilike '%lilly%'          then 'LLY'
                when ob.applicant_full_name ilike '%pfizer%'         then 'PFE'
                when ob.applicant_full_name ilike '%janssen%'        then 'JNJ'
                when ob.applicant_full_name ilike '%abbvie%'         then 'ABBV'
                when ob.applicant_full_name ilike '%novartis%'       then 'NVS'
            end
        ) as ticker,

        -- Flag which tier resolved the match (useful for audit / data quality)
        case
            when ob.trade_name ilike any ('%keytruda%','%januvia%','%opdivo%','%eliquis%',
                '%tagrisso%','%farxiga%','%mounjaro%','%trulicity%','%paxlovid%',
                '%xeljanz%','%stelara%','%darzalex%','%humira%','%skyrizi%',
                '%rinvoq%','%entresto%','%kisqali%')           then 'trade_name'
            when ob.applicant_full_name ilike any ('%merck%','%bristol%','%astrazeneca%',
                '%lilly%','%pfizer%','%janssen%','%abbvie%','%novartis%') then 'applicant_name'
            else 'unmatched'
        end as match_tier

    from ob
),

final as (
    select
        m.trade_name,
        m.ingredient,
        m.applicant_full_name,
        m.appl_no,
        m.exclusivity_code,
        m.exclusivity_date,
        m.ticker,
        m.match_tier,
        cl.company_name
    from mapped m
    inner join company_lookup cl on cl.ticker = m.ticker
    where m.ticker is not null
)

select * from final
