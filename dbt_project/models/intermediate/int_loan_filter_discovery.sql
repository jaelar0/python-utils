-- int_loan_filter_discovery.sql
-- PURPOSE: Reverse-engineer the CRE NOO filter by comparing ALL Core loans
-- against both manually-produced Input Datasets (3-31-2025 and 12-31-2025).
--
-- Every Core loan is flagged with:
--   in_march_input  — loan_id found in the 3-31-2025 Input Dataset
--   in_dec_input    — loan_id found in the 12-31-2025 Input Dataset
--   in_either_input — loan_id found in either
--
-- All candidate filter fields are kept so you can group/pivot them
-- in fct_filter_pattern_analysis to find what consistently predicts membership.
--
-- How to use this model:
--   1. Run: dbt run --select int_loan_filter_discovery
--   2. Query: SELECT * FROM int_loan_filter_discovery WHERE in_either_input = true LIMIT 100
--      → Inspect the fields on loans that ARE in the Input. What do they have in common?
--   3. Query: SELECT * FROM int_loan_filter_discovery WHERE in_either_input = false
--      → What fields are different on loans NOT in the Input?
--   4. Use fct_filter_pattern_analysis for aggregated breakdowns.

with all_core_loans as (

    -- Pull ALL commercial loans from Core — no CRE NOO filter yet
    -- We want to see the full population to find what makes the subset unique
    select * from {{ ref('stg_core_loans') }}
    where as_of_date in ('2025-03-31', '2025-12-31')  -- both periods

),

march_input as (

    select distinct loan_id as loan_id
    from {{ ref('input_dataset_3312025') }}
    where loan_id is not null

),

dec_input as (

    select distinct loan_id as loan_id
    from {{ ref('input_dataset_12312025') }}
    where loan_id is not null

),

joined as (

    select
        l.*,

        -- Input Dataset membership flags
        case when m.loan_id is not null then true else false end     as in_march_input,
        case when d.loan_id is not null then true else false end     as in_dec_input,
        case
            when m.loan_id is not null or d.loan_id is not null
            then true else false
        end                                                         as in_either_input,
        case
            when m.loan_id is not null and d.loan_id is not null
            then true else false
        end                                                         as in_both_inputs,

        -- Convenience label for easier filtering/reporting
        case
            when m.loan_id is not null and d.loan_id is not null then 'BOTH'
            when m.loan_id is not null then 'MARCH_ONLY'
            when d.loan_id is not null then 'DEC_ONLY'
            else 'NEITHER'
        end                                                         as input_membership

    from all_core_loans l
    left join march_input m on l.loan_id = m.loan_id
    left join dec_input   d on l.loan_id = d.loan_id

)

select * from joined
