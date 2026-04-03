-- stg_ncino_loans.sql
-- Staging model for the nCino loan data.
-- nCino is a Salesforce-based lending platform; its field naming differs from Core.
-- This model normalizes nCino fields to match the Core loan naming convention.
--
-- MOCK: Replace source table name with actual DuckDB table name.

with source as (

    select * from {{ source('ncino', 'nc_loan') }}

),

renamed as (

    select
        -- nCino uses Salesforce-style IDs (18-char alphanumeric)
        id                                                  as ncino_id,

        -- Cross-reference to Core loan number (if populated)
        ll__loan_number__c                                  as loan_number,         -- maps to Core loan_number
        ll__cif__c                                          as borrower_cif,        -- customer ID

        -- Loan attributes
        ll__loan_type__c                                    as loan_type_code,
        ll__property_type__c                                as property_type,
        ll__occupancy_type__c                               as occupancy_type_code,
        ll__product_type__c                                 as product_type,
        ll__stage__c                                        as loan_status,

        -- Balances
        cast(ll__committed_amount__c as double)             as committed_balance,
        cast(ll__outstanding_principal__c as double)        as outstanding_balance,

        -- Dates
        cast(ll__original_close_date__c as date)            as origination_date,
        cast(ll__maturity_date__c as date)                  as maturity_date,

        -- Term fields
        cast(ll__amortization_term__c as integer)           as original_amortization_term_months,
        cast(ll__io_period__c as integer)                   as io_period_months,

        -- Interest rate
        ll__rate_type__c                                    as rate_type,
        cast(ll__interest_rate__c as double)                as interest_rate,

        -- nCino-specific fields useful for ARC/Construction identification
        ll__loan_phase__c                                   as loan_phase,          -- 'CONSTRUCTION', 'PERMANENT', 'MINI-PERM'
        cast(ll__construction_to_perm_date__c as date)      as conversion_date,     -- ARC: when loan converts to perm

        recordtypename                                      as record_type,
        as_of_date                                          as as_of_date

    from source

)

select * from renamed
