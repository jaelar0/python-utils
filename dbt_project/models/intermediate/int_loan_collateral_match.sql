-- int_loan_collateral_match.sql
-- Joins CRE NOO loans to their collateral records.
-- Applies address validation flags.
-- Handles multiple collaterals per loan by selecting the primary collateral.
--
-- NOTE: If a loan has no primary_flag set, the model falls back to the
-- most recently appraised collateral. Confirm the business rule with the team.

with loans as (

    select * from {{ ref('int_cre_noo_loans') }}

),

collaterals as (

    select * from {{ ref('stg_core_collaterals') }}

),

-- Filter to real estate collateral only and apply address validation
re_collaterals as (

    select
        *,

        -- Address validation flags
        case
            when street_address_raw is null or trim(street_address_raw) = ''
            then true else false
        end as is_null_address,

        case
            when upper(street_address_raw) ilike 'P.O.%'
              or upper(street_address_raw) ilike 'PO BOX%'
              or upper(street_address_raw) ilike 'P O BOX%'
            then true else false
        end as is_po_box,

        -- Street number present (starts with digits)
        case
            when regexp_matches(trim(street_address_raw), '^\d+')
            then true else false
        end as has_street_number,

        -- Full street address pattern (street number + street name + type abbreviation)
        case
            when regexp_matches(
                trim(street_address_raw),
                '^\d+[A-Za-z]?\s+[\w\s\.\-]{2,}\s+(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Court|Ct|Lane|Ln|Way|Place|Pl|Circle|Cir|Terrace|Ter|Highway|Hwy|Parkway|Pkwy|Pike|Loop|Trail|Trl|Run|Pass|Commons?|Plaza|Center|Centre|Square|Sq)\.?(\s+.+)?$'
            ) then true else false
        end as is_valid_street_address,

        -- ZIP code validation
        case
            when regexp_matches(trim(zip_code_raw), '^\d{5}(-\d{4})?$')
            then true else false
        end as is_valid_zip,

        -- Clean versions of address fields
        trim(street_address_raw)    as street_address,
        trim(city_raw)              as city,
        upper(trim(state_code_raw)) as state_code,
        trim(zip_code_raw)          as zip_code,
        left(trim(zip_code_raw), 5) as zip5

    from collaterals
    where collateral_type_code in ('RE', 'REAL_ESTATE', 'CRE', 'LAND')  -- adjust codes as needed
      and as_of_date = '2025-12-31'

),

-- Rank collaterals per loan: primary first, then most recently appraised
ranked_collaterals as (

    select
        rc.*,
        row_number() over (
            partition by rc.loan_id
            order by
                case when upper(rc.primary_flag::varchar) in ('Y', 'YES', 'TRUE', '1') then 0 else 1 end,
                rc.appraisal_date desc nulls last,
                rc.appraised_value desc nulls last
        ) as collateral_rank

    from re_collaterals rc

),

primary_collaterals as (

    select * from ranked_collaterals
    where collateral_rank = 1

),

-- Join loans to their primary collateral
loan_collateral_joined as (

    select
        l.loan_id,
        l.loan_number,
        l.loan_type_code,
        l.call_report_code,
        l.occupancy_type_code,
        l.product_type,
        l.is_arc_loan,
        l.loan_status,
        l.committed_balance,
        l.outstanding_balance,
        l.origination_date,
        l.effective_origination_date,
        l.maturity_date,
        l.original_amortization_term_months,
        l.io_period_months,
        l.io_end_date,
        l.rate_type,
        l.interest_rate,
        l.branch_code,
        l.loan_officer_id,

        -- Collateral fields
        c.collateral_id,
        c.appraised_value,
        c.appraisal_date,
        c.book_value,
        c.street_address,
        c.city,
        c.state_code,
        c.zip_code,
        c.zip5,

        -- Address validation flags
        c.is_null_address,
        c.is_po_box,
        c.has_street_number,
        c.is_valid_street_address,
        c.is_valid_zip,

        -- Match status flags
        case when c.collateral_id is not null then true else false end as has_collateral_match,
        case
            when c.collateral_id is not null
             and c.is_valid_street_address = true
             and c.is_valid_zip = true
             and c.appraised_value is not null
             and c.appraisal_date is not null
            then true else false
        end as has_complete_collateral_data

    from loans l
    left join primary_collaterals c
        on l.loan_id = c.loan_id

)

select * from loan_collateral_joined
