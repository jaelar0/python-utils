-- assert_collateral_match_rate.sql
-- Custom dbt test: fails if collateral match rate falls below 98% by count.
-- Adjust the threshold (0.98) based on team agreement.
--
-- A failing test means the loan-collateral join needs investigation.

with stats as (

    select
        count(*)                                                    as total_loans,
        sum(case when has_collateral_match then 1 else 0 end)       as matched_loans,
        sum(case when has_collateral_match then 1 else 0 end) * 1.0
            / nullif(count(*), 0)                                   as match_rate

    from {{ ref('fct_cre_noo_stress_test_input') }}

)

-- dbt custom tests return rows when they FAIL.
-- Return a row if match rate is below threshold.
select
    total_loans,
    matched_loans,
    match_rate,
    'Collateral match rate ' || round(match_rate * 100, 2) || '% is below 98% threshold' as failure_reason
from stats
where match_rate < 0.98
