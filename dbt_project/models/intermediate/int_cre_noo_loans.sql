-- int_cre_noo_loans.sql
-- Filters the full commercial loan population down to CRE Non-Owner Occupied loans.
-- Also handles ARC (construction-to-perm) loan classification.
--
-- KEY BUSINESS RULE: Adjust the filter conditions below once the team confirms
-- which field(s) and values identify CRE NOO in the Core system.
-- Common options (use whichever applies):
--   Option A: call_report_code = '1-a'       (FFIEC Call Report — most reliable)
--   Option B: occupancy_type_code = 'NOO'
--   Option C: owner_occupied_flag = false
--   Option D: combination of loan_type + collateral_type
--
-- Validate by comparing output loan IDs against the 12-31-2025 Input Dataset.

with core_loans as (

    select * from {{ ref('stg_core_loans') }}

),

ncino_loans as (

    select * from {{ ref('stg_ncino_loans') }}

),

-- Step 1: Identify ARC / Construction-to-Perm loans from nCino
-- These require special handling: use perm terms, not construction terms.
arc_flags as (

    select
        loan_number,
        loan_phase,
        conversion_date,
        case
            when loan_phase in ('CONSTRUCTION', 'MINI-PERM')
                or product_type ilike '%ARC%'
                or product_type ilike '%CONSTRUCTION%'
            then true
            else false
        end as is_arc_loan

    from ncino_loans
    where loan_number is not null

),

-- Step 2: Filter Core loans to CRE NOO universe
-- *** ADJUST THESE CONDITIONS BASED ON ACTUAL FIELD VALUES IN THE DATA ***
cre_noo_filtered as (

    select
        l.*,
        arc.is_arc_loan,
        arc.loan_phase,
        arc.conversion_date,

        -- Effective origination date:
        -- For ARC loans, amortization begins at conversion, not construction close
        case
            when arc.is_arc_loan = true and arc.conversion_date is not null
            then arc.conversion_date
            else l.origination_date
        end as effective_origination_date

    from core_loans l
    left join arc_flags arc
        on l.loan_number = arc.loan_number

    where
        -- *** PRIMARY FILTER — choose one of the options below ***
        -- Option A (preferred if call report codes are populated):
        l.call_report_code = '1-a'

        -- Option B (fallback):
        -- l.occupancy_type_code = 'NOO'
        -- and l.loan_type_code in ('CRE', 'RE', 'COMM_RE')

        -- Option C (if owner_occupied_flag is boolean):
        -- l.owner_occupied_flag = false
        -- and l.loan_type_code in ('CRE', 'RE')

        -- Exclude loans that are paid off or charged off
        and l.loan_status not in ('PAID_OFF', 'CHARGED_OFF', 'CLOSED')

        -- Only real-estate collateral types
        -- (confirm exact code values in your data)
        -- and l.collateral_type_code in ('RE', 'REAL_ESTATE', 'CRE')

        -- Filter to target period
        and l.as_of_date = '2025-12-31'

)

select * from cre_noo_filtered
