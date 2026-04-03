-- normalize_state.sql
-- Normalizes a state field to its 2-letter USPS abbreviation.
-- Handles:
--   • Already-abbreviated values: 'FL', 'fl', ' FL '  → 'FL'
--   • Full names: 'Florida', 'FLORIDA', 'florida'      → 'FL'
--   • Common typos/alternatives included in the seed table
--   • NULL and blank values → NULL
--
-- Depends on the `state_abbreviations` seed being loaded (dbt seed).
--
-- Usage in a model:
--   {{ normalize_state('state_code_raw') }} as state_code
--
-- Or inline in a select:
--   select
--       loan_id,
--       {{ normalize_state('state_field') }} as state_code
--   from my_table

{% macro normalize_state(state_col) %}

    case
        -- NULL / blank → NULL
        when {{ state_col }} is null or trim({{ state_col }}) = ''
        then null

        -- Already a valid 2-letter abbreviation (case-insensitive)
        when upper(trim({{ state_col }})) in (
            select upper(abbreviation) from {{ ref('state_abbreviations') }}
        )
        then upper(trim({{ state_col }}))

        -- Full name match (case-insensitive) → look up abbreviation
        else (
            select upper(sa.abbreviation)
            from {{ ref('state_abbreviations') }} sa
            where upper(sa.full_name) = upper(trim({{ state_col }}))
            limit 1
        )

    end

{% endmacro %}
