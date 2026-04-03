-- validate_address.sql
-- Macro that returns address validation boolean columns for a collateral record.
--
-- Usage:
--   {{ validate_address('street_address_raw', 'zip_code_raw') }}

{% macro validate_address(street_col, zip_col) %}

    -- NULL or blank address
    case
        when {{ street_col }} is null or trim({{ street_col }}) = ''
        then true else false
    end                                                         as is_null_address,

    -- PO Box (not a real property address)
    case
        when upper(trim({{ street_col }})) ilike 'P.O.%'
          or upper(trim({{ street_col }})) ilike 'PO BOX%'
          or upper(trim({{ street_col }})) ilike 'P O BOX%'
          or upper(trim({{ street_col }})) ilike 'POST OFFICE BOX%'
        then true else false
    end                                                         as is_po_box,

    -- Has a leading street number
    case
        when regexp_matches(trim({{ street_col }}), '^\d+[A-Za-z]?\s')
        then true else false
    end                                                         as has_street_number,

    -- Full street address pattern match
    -- Requires: number, space, name text, space, recognized street suffix
    case
        when regexp_matches(
            trim({{ street_col }}),
            '^[0-9]+[A-Za-z]?\s+[\w\s\.\-&'']{2,}\s+'
            || '(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Court|Ct|'
            || 'Lane|Ln|Way|Place|Pl|Circle|Cir|Terrace|Ter|Highway|Hwy|'
            || 'Parkway|Pkwy|Pike|Loop|Trail|Trl|Run|Pass|Commons?|Plaza|'
            || 'Center|Centre|Square|Sq|Cove|Cv|Creek|Crk|Crossing|Xing|'
            || 'Expressway|Expy|Freeway|Fwy|Glen|Gln|Grove|Grv|'
            || 'Heights|Hts|Hollow|Holw|Junction|Jct|Key|Ky|Knoll|Knl)'
            || '\.?(\s+.+)?$'
        )
        then true else false
    end                                                         as is_valid_street_address,

    -- ZIP code format: 5-digit or ZIP+4
    case
        when regexp_matches(trim({{ zip_col }}), '^\d{5}(-\d{4})?$')
        then true else false
    end                                                         as is_valid_zip

{% endmacro %}
