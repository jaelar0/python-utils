-- stg_core_loans.sql
-- Staging model for the Core system loan table.
-- Light cleaning only: rename columns to snake_case, cast types, no business logic.
--
-- MOCK: Replace <core_schema>.<loans_table> with the actual DuckDB table name
-- discovered via: SHOW TABLES; and DESCRIBE <table>;

with source as (

    select * from {{ source('core', 'loans') }}

),

renamed as (

    select
        -- identifiers
        loan_id                                         as loan_id,
        loan_number                                     as loan_number,

        -- loan attributes
        loan_type_code                                  as loan_type_code,
        loan_type_description                           as loan_type_description,
        call_report_code                                as call_report_code,    -- FFIEC code; '1-a' = CRE NOO
        occupancy_type_code                             as occupancy_type_code, -- 'NOO', 'OO', etc.
        owner_occupied_flag                             as owner_occupied_flag,
        product_type                                    as product_type,        -- watch for 'ARC', 'CONSTRUCTION'
        loan_status                                     as loan_status,         -- 'ACTIVE', 'CONSTRUCTION', 'MATURED'
        purpose_code                                    as purpose_code,

        -- balances
        cast(committed_balance as double)               as committed_balance,
        cast(outstanding_balance as double)             as outstanding_balance,
        cast(unfunded_balance as double)                as unfunded_balance,

        -- dates
        cast(origination_date as date)                  as origination_date,
        cast(maturity_date as date)                     as maturity_date,
        cast(modification_date as date)                 as modification_date,   -- may be null

        -- term fields
        cast(original_amortization_term as integer)     as original_amortization_term_months,
        cast(io_period_months as integer)               as io_period_months,    -- null if not IO loan
        cast(io_end_date as date)                       as io_end_date,         -- null if not IO loan

        -- interest rate
        rate_type                                       as rate_type,           -- 'FIXED', 'VARIABLE', 'HYBRID'
        cast(interest_rate as double)                   as interest_rate,
        index_type                                      as index_type,          -- 'SOFR', 'PRIME', etc.
        cast(spread as double)                          as spread,

        -- administrative
        branch_code                                     as branch_code,
        loan_officer_id                                 as loan_officer_id,
        as_of_date                                      as as_of_date

    from source

)

select * from renamed
