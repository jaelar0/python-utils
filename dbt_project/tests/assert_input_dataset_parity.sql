-- assert_input_dataset_parity.sql
-- Validation test: compares the dbt-produced Input Dataset against the
-- manually-produced 12-31-2025 Input Dataset that the team provided.
--
-- SETUP: Load the provided Input Dataset into DuckDB as a seed or source table.
-- Name it: input_dataset_manual_20251231
--
-- This test fails if there are loan IDs in one dataset but not the other.
-- Investigate discrepancies: are they missing from manual (over-capture) or
-- missing from dbt (under-capture)?

with dbt_loans as (

    select loan_id, loan_number
    from {{ ref('fct_cre_noo_stress_test_input') }}

),

manual_loans as (

    -- The reference Input Dataset provided by the team
    select loan_id, loan_number
    from {{ source('validation', 'input_dataset_manual_20251231') }}

),

-- Loans in manual dataset but not in dbt output (under-capture)
missing_from_dbt as (

    select
        m.loan_id,
        m.loan_number,
        'In manual, missing from dbt (under-capture)' as discrepancy_type
    from manual_loans m
    left join dbt_loans d on m.loan_id = d.loan_id
    where d.loan_id is null

),

-- Loans in dbt output but not in manual dataset (over-capture)
extra_in_dbt as (

    select
        d.loan_id,
        d.loan_number,
        'In dbt, missing from manual (over-capture)' as discrepancy_type
    from dbt_loans d
    left join manual_loans m on d.loan_id = m.loan_id
    where m.loan_id is null

)

select * from missing_from_dbt
union all
select * from extra_in_dbt
