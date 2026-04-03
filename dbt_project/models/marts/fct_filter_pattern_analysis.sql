-- fct_filter_pattern_analysis.sql
-- PURPOSE: Surface the patterns that distinguish loans in the Input Datasets
-- from the full Core population. One row per unique combination of candidate
-- filter fields, showing coverage in each Input period.
--
-- How to read this table:
--   • Sort by dec_input_count DESC to see which field combinations are most common in the target.
--   • A field value where dec_input_pct ≈ 100% but total_core_count >> dec_input_count
--     means that value is NECESSARY but not SUFFICIENT to identify the target.
--   • A combination where dec_input_pct = 100% AND total_core_count ≈ dec_input_count
--     is a reliable filter candidate.
--
-- After reviewing this output, update int_cre_noo_loans.sql with the confirmed filter.

with base as (

    select * from {{ ref('int_loan_filter_discovery') }}

),

-- ─────────────────────────────────────────────────────────────────
-- Section 1: Breakdown by individual candidate filter fields
-- ─────────────────────────────────────────────────────────────────

by_call_report_code as (

    select
        'call_report_code'                          as filter_field,
        coalesce(call_report_code, '(null)')        as field_value,
        count(*)                                    as total_core_count,
        sum(case when in_march_input then 1 else 0 end) as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end) as dec_input_count,
        sum(case when in_either_input then 1 else 0 end) as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                    as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by call_report_code

),

by_occupancy_type as (

    select
        'occupancy_type_code'                               as filter_field,
        coalesce(occupancy_type_code, '(null)')             as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by occupancy_type_code

),

by_loan_type as (

    select
        'loan_type_code'                                    as filter_field,
        coalesce(loan_type_code, '(null)')                  as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by loan_type_code

),

by_product_type as (

    select
        'product_type'                                      as filter_field,
        coalesce(product_type, '(null)')                    as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by product_type

),

by_loan_status as (

    select
        'loan_status'                                       as filter_field,
        coalesce(loan_status, '(null)')                     as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by loan_status

),

by_purpose_code as (

    select
        'purpose_code'                                      as filter_field,
        coalesce(purpose_code, '(null)')                    as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by purpose_code

),

-- ─────────────────────────────────────────────────────────────────
-- Section 2: Multi-field combinations — the most diagnostic view
-- Find the exact combination(s) that perfectly captures the target
-- ─────────────────────────────────────────────────────────────────

by_combo as (

    select
        'COMBO: call_report + occupancy + loan_type'        as filter_field,
        coalesce(call_report_code,   '(null)') || ' | '
            || coalesce(occupancy_type_code, '(null)') || ' | '
            || coalesce(loan_type_code,      '(null)')      as field_value,
        count(*)                                            as total_core_count,
        sum(case when in_march_input then 1 else 0 end)     as march_input_count,
        sum(case when in_dec_input   then 1 else 0 end)     as dec_input_count,
        sum(case when in_either_input then 1 else 0 end)    as either_input_count,
        round(sum(case when in_dec_input then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as dec_input_pct,
        sum(outstanding_balance)                            as total_outstanding_balance
    from base
    where as_of_date = '2025-12-31'
    group by call_report_code, occupancy_type_code, loan_type_code

),

-- ─────────────────────────────────────────────────────────────────
-- Section 3: Loans in Input Datasets that are NOT in Core
-- These represent data gaps and should be investigated
-- ─────────────────────────────────────────────────────────────────

missing_from_core as (

    select
        'MISSING FROM CORE'                         as filter_field,
        d.loan_id                                   as field_value,
        0                                           as total_core_count,
        0                                           as march_input_count,
        1                                           as dec_input_count,
        1                                           as either_input_count,
        null                                        as dec_input_pct,
        null                                        as total_outstanding_balance
    from {{ ref('input_dataset_12312025') }} d
    left join {{ ref('stg_core_loans') }} c
        on d.loan_id = c.loan_id
        and c.as_of_date = '2025-12-31'
    where c.loan_id is null

),

-- Union all sections
unioned as (
    select * from by_call_report_code
    union all
    select * from by_occupancy_type
    union all
    select * from by_loan_type
    union all
    select * from by_product_type
    union all
    select * from by_loan_status
    union all
    select * from by_purpose_code
    union all
    select * from by_combo
    union all
    select * from missing_from_core
)

select
    filter_field,
    field_value,
    total_core_count,
    march_input_count,
    dec_input_count,
    either_input_count,
    dec_input_pct,
    total_outstanding_balance,

    -- Signal strength: how well does this value predict Input membership?
    -- 100% = every loan with this value is in the Input (perfect positive signal)
    -- 0%   = no loans with this value are in the Input
    case
        when dec_input_pct = 100 and total_core_count > 0 then 'STRONG — all loans captured'
        when dec_input_pct >= 90 then 'HIGH — mostly captured, minor leakage'
        when dec_input_pct >= 50 then 'MEDIUM — partial signal, likely needs additional filter'
        when dec_input_pct > 0   then 'WEAK — mostly excluded, not a good filter'
        else 'NONE — no loans captured'
    end                                                     as signal_strength

from unioned
order by filter_field, dec_input_count desc
