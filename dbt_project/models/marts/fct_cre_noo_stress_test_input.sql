-- fct_cre_noo_stress_test_input.sql
-- Final mart: the "Input Dataset" for the third-party vendor stress-test model.
-- This is the table that replaces the manually-produced Input Dataset.
--
-- Period: 12-31-2025
-- Scope: CRE Non-Owner Occupied loans with collateral, terms, and address data

with base as (

    select * from {{ ref('int_loan_collateral_match') }}

),

-- Calculate remaining terms as of the report date
-- The report date is parameterized here for easy period-over-period reuse.
-- To run for 3-31-2025, change the as_of_date variable in dbt_project.yml
with_terms as (

    select
        *,
        {{ calculate_remaining_terms('maturity_date', 'effective_origination_date', 'original_amortization_term_months', 'io_end_date') }}

    from base

),

-- Final output shaped for vendor input format
final as (

    select
        -- === LOAN IDENTIFIERS ===
        loan_id,
        loan_number,

        -- === LOAN ATTRIBUTES ===
        loan_type_code,
        call_report_code,
        occupancy_type_code,
        product_type,
        is_arc_loan,
        loan_status,
        rate_type,
        interest_rate,

        -- === BALANCES ===
        committed_balance,
        outstanding_balance,

        -- === DATES ===
        origination_date,
        effective_origination_date,
        maturity_date,

        -- === TERM FIELDS (vendor inputs) ===
        original_amortization_term_months,
        io_period_months,
        io_end_date,
        months_to_maturity,
        amortization_term_remaining,
        io_term_remaining,
        is_matured,
        is_io_expired,

        -- === COLLATERAL ===
        collateral_id,
        appraised_value,
        appraisal_date,
        book_value,

        -- === ADDRESS ===
        street_address,
        city,
        state_code,
        zip_code,
        zip5,

        -- === DATA QUALITY FLAGS ===
        has_collateral_match,
        has_complete_collateral_data,
        is_null_address,
        is_po_box,
        has_street_number,
        is_valid_street_address,
        is_valid_zip,

        -- === ADMIN ===
        branch_code,
        loan_officer_id,

        -- Report date
        date '2025-12-31'   as report_date,
        current_timestamp   as dbt_loaded_at

    from with_terms

)

select * from final
