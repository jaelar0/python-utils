-- stg_core_collaterals.sql
-- Staging model for the Core system collateral table.
-- Includes both the collateral master and the loan-collateral cross-reference.
--
-- MOCK: Replace source table names with actual DuckDB table names.

with collateral_source as (

    select * from {{ source('core', 'collaterals') }}

),

xref_source as (

    -- Bridge table: one row per loan_id / collateral_id pairing
    select * from {{ source('core', 'loan_collateral_xref') }}

),

collaterals_renamed as (

    select
        collateral_id                                       as collateral_id,
        collateral_type_code                                as collateral_type_code,   -- 'RE' = Real Estate
        collateral_description                              as collateral_description,
        primary_flag                                        as primary_flag,           -- 'Y'/'N' or boolean

        -- collateral value
        cast(appraised_value as double)                     as appraised_value,
        cast(appraisal_date as date)                        as appraisal_date,
        cast(book_value as double)                          as book_value,

        -- address fields
        -- state_code is normalized here via macro (handles 'FL' and 'Florida' equally)
        street_address                                      as street_address_raw,
        city                                                as city_raw,
        {{ normalize_state('state_code') }}                 as state_code,   -- normalized to 2-letter abbrev
        state_code                                          as state_code_raw,
        zip_code                                            as zip_code_raw,

        as_of_date                                          as as_of_date

    from collateral_source

),

xref_renamed as (

    select
        loan_id         as loan_id,
        collateral_id   as collateral_id,
        primary_flag    as xref_primary_flag   -- some systems store primary flag here, not on collateral
    from xref_source

)

-- Final output: join xref to collateral detail
select
    x.loan_id,
    x.collateral_id,
    coalesce(c.primary_flag, x.xref_primary_flag)   as primary_flag,
    c.collateral_type_code,
    c.collateral_description,
    c.appraised_value,
    c.appraisal_date,
    c.book_value,
    c.street_address_raw,
    c.city_raw,
    c.state_code,       -- normalized 2-letter abbreviation
    c.state_code_raw,   -- original value as it came from Core
    c.zip_code_raw,
    c.as_of_date

from xref_renamed x
left join collaterals_renamed c
    on x.collateral_id = c.collateral_id
