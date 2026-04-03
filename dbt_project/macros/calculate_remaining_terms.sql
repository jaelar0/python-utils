-- calculate_remaining_terms.sql
-- Macro that returns remaining term columns for a loan as of a given report date.
-- Handles IO loans, ARC loans (uses effective_origination_date), and past-maturity loans.
--
-- Usage in a model:
--   {{ calculate_remaining_terms('maturity_date', 'effective_origination_date', 'original_amortization_term_months', 'io_end_date') }}

{% macro calculate_remaining_terms(
    maturity_date_col,
    origination_date_col,
    amort_term_col,
    io_end_date_col,
    as_of_date="date '2025-12-31'"
) %}

    -- Months from report date to maturity date
    -- Negative = loan is past its stated maturity (flag it, don't show negative)
    datediff(
        'month',
        {{ as_of_date }},
        {{ maturity_date_col }}
    )                                                           as months_to_maturity_raw,

    greatest(0,
        datediff('month', {{ as_of_date }}, {{ maturity_date_col }})
    )                                                           as months_to_maturity,

    -- Past-maturity flag
    case
        when {{ maturity_date_col }} <= {{ as_of_date }} then true
        else false
    end                                                         as is_matured,

    -- Elapsed months from effective origination to report date
    datediff(
        'month',
        {{ origination_date_col }},
        {{ as_of_date }}
    )                                                           as months_since_origination,

    -- Amortization term remaining
    -- For IO loans: amortization clock starts at IO end date, not origination
    -- For non-IO loans: clock starts at origination
    case
        when {{ amort_term_col }} is null then null
        when {{ io_end_date_col }} is not null and {{ io_end_date_col }} > {{ as_of_date }}
            -- Still in IO period: full amortization term remains
            then {{ amort_term_col }}
        when {{ io_end_date_col }} is not null and {{ io_end_date_col }} <= {{ as_of_date }}
            -- IO period has expired; amortization clock started at IO end
            then greatest(0,
                {{ amort_term_col }} - datediff('month', {{ io_end_date_col }}, {{ as_of_date }})
            )
        else
            -- Standard loan: amortization clock from origination
            greatest(0,
                {{ amort_term_col }} - datediff('month', {{ origination_date_col }}, {{ as_of_date }})
            )
    end                                                         as amortization_term_remaining,

    -- Interest only term remaining
    case
        when {{ io_end_date_col }} is null then null          -- not an IO loan
        when {{ io_end_date_col }} <= {{ as_of_date }} then 0 -- IO period already expired
        else datediff('month', {{ as_of_date }}, {{ io_end_date_col }})
    end                                                         as io_term_remaining,

    -- IO expiry flag
    case
        when {{ io_end_date_col }} is null then false
        when {{ io_end_date_col }} <= {{ as_of_date }} then true
        else false
    end                                                         as is_io_expired

{% endmacro %}
