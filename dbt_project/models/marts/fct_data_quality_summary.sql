-- fct_data_quality_summary.sql
-- Aggregated data quality metrics for the PowerBI dashboard.
-- One row per report_date. Add new periods by re-running dbt with different as_of dates.

with input as (

    select * from {{ ref('fct_cre_noo_stress_test_input') }}

),

summary as (

    select
        report_date,

        -- Loan counts
        count(*)                                                            as total_loans,
        sum(outstanding_balance)                                            as total_outstanding_balance,
        sum(committed_balance)                                              as total_committed_balance,

        -- Collateral match
        sum(case when has_collateral_match then 1 else 0 end)              as loans_with_collateral,
        sum(case when not has_collateral_match then 1 else 0 end)          as loans_without_collateral,
        round(
            sum(case when has_collateral_match then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                                   as collateral_match_pct,

        -- Complete collateral data
        sum(case when has_complete_collateral_data then 1 else 0 end)      as loans_with_complete_collateral,
        round(
            sum(case when has_complete_collateral_data then 1 else 0 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                                   as complete_collateral_pct,

        -- Address quality (for loans with collateral)
        sum(case when has_collateral_match and is_valid_street_address then 1 else 0 end)   as valid_addresses,
        sum(case when has_collateral_match and is_null_address then 1 else 0 end)            as null_addresses,
        sum(case when has_collateral_match and is_po_box then 1 else 0 end)                  as po_box_addresses,
        sum(case when has_collateral_match and not is_valid_street_address
                      and not is_null_address and not is_po_box then 1 else 0 end)           as invalid_format_addresses,
        round(
            sum(case when has_collateral_match and is_valid_street_address then 1 else 0 end) * 100.0
            / nullif(sum(case when has_collateral_match then 1 else 0 end), 0), 2
        )                                                                   as valid_address_pct,

        -- Term data completeness
        sum(case when months_to_maturity is not null then 1 else 0 end)    as loans_with_maturity_term,
        sum(case when amortization_term_remaining is not null then 1 else 0 end) as loans_with_amort_term,
        sum(case when is_matured then 1 else 0 end)                        as matured_loans,
        sum(case when is_arc_loan then 1 else 0 end)                       as arc_loans,

        -- Appraisal staleness (appraisal > 2 years old as of report date)
        sum(case
            when appraisal_date is not null
             and appraisal_date < report_date - interval '2 years'
            then 1 else 0
        end)                                                                as stale_appraisals

    from input
    group by report_date

)

select * from summary
