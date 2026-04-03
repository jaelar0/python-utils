# Task 1: Analyst Notes & Approach

**As of:** 2026-04-03
**Reference period:** 12-31-2025 (primary), 3-31-2025 (secondary validation)

---

## 1. Where and How to Start

### Is DuckDB + dbt the right approach?

**Yes — this is a sound architecture for this situation.** The constraints are:
- No write access to Snowflake (read-only)
- Need a reproducible, documented pipeline
- No centralized server-side compute available to the team

DuckDB runs entirely in-process, reads Parquet/CSV/JSON natively, and the `dbt-duckdb` adapter is production-stable. This is an increasingly common pattern for local analytical workflows and is a legitimate starting point before the team potentially earns write access to Snowflake or migrates to a proper Snowflake dbt project later.

The dbt project structure also directly addresses the documentation gap the team has — each model has a `schema.yml` description, and the DAG makes the lineage explicit.

### First steps

1. **Inventory what you have in DuckDB.** Run `SHOW TABLES;` and `DESCRIBE <table>;` on each table. Identify which tables come from "Core" vs. nCino. Document column names, data types, and row counts.

2. **Open both Input Datasets side-by-side.** These are the ground truth. Before writing any SQL, map every column in the 12-31-2025 Input Dataset back to a source column in the DuckDB tables. This is your reverse-engineering starting point.

3. **Diff the two Input Datasets (3-31-2025 vs. 12-31-2025).** Look for columns that appear/disappear, naming changes, and value format changes. This tells you whether the manual process has been consistent.

4. **Identify the loan universe first, then add fields.** It is far easier to get the filter logic right on a simple loan-level query before joining in collateral, terms, and address data.

---

## 1b. Output Database Architecture

The final mart tables are written to a **separate DuckDB file** (`cre_stress_test_output.duckdb`). This separation has practical benefits:

- The source DuckDB file (Snowflake pull) is large and gets refreshed periodically — no reason for PowerBI or the vendor to connect to it directly
- The output file contains only the clean, processed final tables — a much smaller surface area
- `cre_stress_test_output.duckdb` is what you hand to the vendor and connect PowerBI to

Configured in `profiles.yml` via dbt-duckdb's `attach` feature. All `marts/` models write to `output_db` automatically (set in `dbt_project.yml`).

---

## 2. Filtering Down to CRE Non-Owner Occupied (NOO)

### How to reverse-engineer the filter (new models)

Rather than guessing what fields the team used, two new models let you discover the filter empirically:

**`int_loan_filter_discovery`** — Every Core loan flagged with whether it appears in the 3-31 Input, the 12-31 Input, both, or neither. All candidate filter columns are preserved. Run this and browse the `BOTH` rows — what do they have in common?

**`fct_filter_pattern_analysis`** — Aggregated by each candidate field (call_report_code, occupancy_type_code, loan_type_code, product_type, loan_status, purpose_code, and a multi-field combo). For each unique value you see `dec_input_pct` — how many loans with that value appear in the 12-31 Input. A `STRONG` signal means 100% of loans with that value are in the Input.

**Workflow:**
```bash
dbt seed                                        # load Input Dataset CSVs and state lookup
dbt run --select int_loan_filter_discovery      # build the comparison layer
dbt run --select fct_filter_pattern_analysis    # build the pattern summary
```
Then query:
```sql
-- Find the fields that are a perfect or near-perfect predictor
SELECT filter_field, field_value, total_core_count, dec_input_count, dec_input_pct, signal_strength
FROM output_db.marts.fct_filter_pattern_analysis
WHERE signal_strength IN ('STRONG — all loans captured', 'HIGH — mostly captured, minor leakage')
ORDER BY dec_input_count DESC;
```
The result directly tells you what to put in `int_cre_noo_loans.sql`.

### What to look for

The Core system almost certainly has one or more of these fields that identify the loan segment:

| Field (typical naming) | Values to look for |
|---|---|
| `loan_type` / `loan_type_code` | e.g., `'CRE'`, `'RE'`, `'COMM'` |
| `collateral_type` / `property_type` | e.g., `'COMMERCIAL'`, `'OFFICE'`, `'RETAIL'`, `'INDUSTRIAL'`, `'MULTIFAMILY'` |
| `occupancy_type` / `owner_occupied_flag` | `'N'`, `'NOO'`, `false` |
| `purpose_code` | Acquisition, refinance, construction — typically numeric codes |
| `call_code` / `call_report_code` | FFIEC Call Report codes: `1-a` (CRE NOO) vs. `1-b` (owner-occupied) |

> **Key insight:** FFIEC Call Report codes are the most reliable filter if they're in the data. Code `1-a` (or equivalent) = CRE Non-Owner Occupied. This is how regulators define the segment and how stress-test models typically expect the data to be labeled.

### ARC Loans

"ARC" likely refers to **Adjustable Rate Construction** or possibly an internal product name. These are structured products that commonly have:
- An initial construction/draw period (interest-only or draw-period)
- A conversion to a permanent loan at stabilization
- Two sets of terms: construction terms and perm terms

Watch for: these loans may have `loan_status` = `'CONSTRUCTION'` or a flag like `is_construction = true`. They may appear in the Input Dataset in a specific way (e.g., using perm loan terms, not construction terms). This is a critical edge case to document and verify against the 12-31-2025 Input Dataset.

### Validation approach

After writing the CRE NOO filter, compare your output row count and loan IDs against the 12-31-2025 Input Dataset:
- Loans in Input but NOT in your filter → you are under-capturing
- Loans in your filter but NOT in Input → you are over-capturing
- Target: near-zero discrepancy (small delta acceptable if there are known edge cases)

---

## 3. Matching Loans to Collateral

### The Core system join

Typically: `loan_collateral_xref` or a bridge table where `loan_id` joins to `collateral_id`. This is usually a many-to-one relationship (one collateral securing one loan) for CRE, but cross-collateralization (one collateral, multiple loans) and blanket liens (one loan, multiple collaterals) are both possible.

**Fields needed per collateral:**
- Collateral value (appraised or book)
- Appraisal date
- Street address, city, state, ZIP

### Handling multiple collaterals per loan

If a loan has multiple collaterals, you need a business rule:
- **Most common for stress testing:** use the primary collateral (flagged in the system), or
- **Aggregate:** sum collateral values, use most recent appraisal date, use primary address

Verify which approach the team used in the manual 12-31-2025 dataset.

### Acceptable discrepancy

The team said "a small discrepancy is fine." Define this concretely — e.g., ≤ 2% of loans by count or ≤ 1% by balance. This becomes a dbt test threshold.

---

## 4. State Normalization

State values in Core can arrive as `'FL'`, `'Florida'`, `'FLORIDA'`, `'florida'` — all representing the same state. The `normalize_state` macro in `macros/normalize_state.sql` handles all of these by:

1. Checking if the value is already a valid 2-letter abbreviation (case-insensitive match against the `state_abbreviations` seed)
2. If not, looking up the full name in the seed table and returning the abbreviation
3. NULL/blank → NULL

The seed `seeds/state_abbreviations.csv` covers all 50 states, DC, Puerto Rico, Guam, USVI, American Samoa, and Northern Mariana Islands.

The normalization is applied in `stg_core_collaterals.sql` — both `state_code` (normalized) and `state_code_raw` (original value) are preserved so you can audit discrepancies.

**To add it to the nCino staging model** if nCino also has inconsistent state values:
```sql
{{ normalize_state('ll__state__c') }} as state_code
```

---

## 4. Street Address Validation (Regex)

The analyst needs to validate that what the system returns as "Street Address" is actually a parseable street address and not a PO Box, NULL-filled field, or garbage value.

### Recommended regex pattern

```sql
-- DuckDB / SQL regex for a valid US street address
-- Matches patterns like: 123 Main St, 4500 Oak Avenue Suite 200, 1 Plaza Blvd Apt 3B
regexp_matches(
    street_address,
    '^[0-9]+[A-Za-z]?\s+.{3,}'
)
```

A more thorough pattern:

```
^\d+[A-Za-z]?\s+[\w\s\.\-]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Court|Ct|Lane|Ln|Way|Place|Pl|Circle|Cir|Terrace|Ter|Highway|Hwy|Parkway|Pkwy|Pike|Loop|Trail|Trl|Run|Pass)\.?(?:\s+(?:Apt|Ste|Suite|Unit|#|Floor|Fl)[\s\.]?\w+)?$
```

### Additional validation flags to create

| Flag | Logic |
|---|---|
| `is_po_box` | `ILIKE 'P.O.%' OR ILIKE 'PO BOX%'` |
| `is_null_address` | `street_address IS NULL OR TRIM(street_address) = ''` |
| `is_valid_zip` | `regexp_matches(zip, '^\d{5}(-\d{4})?$')` |
| `is_valid_state` | State code in known 50-state list + DC + territories |
| `has_valid_street_number` | `regexp_matches(street_address, '^\d+')` |

Build these as columns in the collateral model so the PowerBI dashboard can surface them.

---

## 5. Calculating Remaining Terms (as of 12-31-2025)

### Key fields assumed available

- `origination_date`
- `maturity_date`
- `original_amortization_term` (in months)
- `interest_only_end_date` or `io_period_months` (if IO loan)

### Formulas

```sql
-- Maturity Term Remaining (months from 12-31-2025 to Maturity Date)
DATEDIFF('month', DATE '2025-12-31', maturity_date) AS months_to_maturity

-- Elapsed months since origination
DATEDIFF('month', origination_date, DATE '2025-12-31') AS months_elapsed

-- Amortization Term Remaining
-- (original term minus how many months of amortization have passed)
-- NOTE: if loan has an IO period, amortization clock starts after IO ends
GREATEST(0,
    original_amortization_term - DATEDIFF('month', origination_date, DATE '2025-12-31')
) AS amortization_term_remaining

-- Interest Only Term Remaining (if applicable)
CASE
    WHEN io_end_date IS NULL THEN NULL  -- not an IO loan
    WHEN io_end_date <= DATE '2025-12-31' THEN 0  -- IO period already expired
    ELSE DATEDIFF('month', DATE '2025-12-31', io_end_date)
END AS io_term_remaining
```

### Edge cases to handle

- **ARC/Construction loans:** Use the perm loan origination date if applicable, not the construction draw date.
- **Modified loans:** If the loan was modified/extended, origination date may not reflect the current amortization schedule. Check for a `modification_date` field.
- **Negative remaining terms:** A loan past maturity will show a negative `months_to_maturity` — flag these as `is_matured = true` rather than showing negative values.
- **Amortization for IO loans:** The amortization clock should not start until after the IO period ends. If `io_end_date > origination_date`, use `io_end_date` as the start of the amortization clock.

---

## 6. PowerBI Dashboard — Data Quality / Match Rate

### Recommended pages / visuals

**Page 1: Executive Summary**
- % Loans with Collateral Match (gauge)
- % Collaterals with Valid Address (gauge)
- % Loans with Complete Term Data (gauge)
- Total Loan Count and Total Commitment Balance (KPI cards)

**Page 2: Loan-Collateral Match Detail**
- Bar chart: Matched vs. Unmatched by loan officer / branch / region
- Table: Unmatched loans (drillthrough enabled) — loan ID, balance, loan officer
- Trend line: Match rate over time (using both 3-31 and 12-31 periods)

**Page 3: Address Validation**
- Pie/donut: Valid / PO Box / Null / Pattern-fail breakdown
- Map visual: Geocoded addresses for valid records (confirms geographic distribution)
- Table: Failed address records with collateral ID and loan ID for remediation

**Page 4: Term Data Completeness**
- Stacked bar: Loans with/without Maturity Date, Origination Date, Amort Term
- Scatter plot: Remaining Maturity vs. Remaining Amortization (outlier detection)
- Flag: Loans with `months_to_maturity < 0` (past maturity)

### Data source for PowerBI

Export the final mart table from DuckDB to CSV or Parquet and connect PowerBI via:
- DirectQuery to DuckDB (requires ODBC driver — less ideal for local setup)
- **Recommended:** Export to CSV/Parquet → PowerBI Import mode

---

## 7. Overall dbt Project Philosophy

### Layer structure

```
sources (DuckDB tables = raw Snowflake pull)
  └─ staging/     (1:1 with source tables, light cleaning, type casting, rename)
       └─ intermediate/   (joins, business logic, filtering)
            └─ marts/     (final output tables = Input Dataset equivalent)
```

### dbt + DuckDB setup

```bash
pip install dbt-duckdb
dbt init cre_stress_test
# Edit profiles.yml to point at the local .duckdb file
```

**profiles.yml:**
```yaml
cre_stress_test:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/local/data.duckdb
      threads: 4
```

> Since there is no write access to Snowflake, all dbt models write to DuckDB. When/if the team moves to Snowflake, only `profiles.yml` needs to change — the SQL logic stays the same (DuckDB is largely ANSI-compliant with minor dialect differences).

### Testing strategy

- `not_null` and `unique` tests on loan IDs in every model
- `accepted_values` test on occupancy_type, loan_type
- Custom test: match rate ≥ 98% for loan-to-collateral join
- Snapshot model to track period-over-period changes (3-31 vs 12-31)

---

## Open Questions for the Team

1. What field(s) in Core identify CRE NOO vs. owner-occupied? Is it an occupancy flag, a call report code, or a product type code?
2. How are ARC loans expected to appear in the Input Dataset — as construction loans or as their permanent terms?
3. When a loan has multiple collaterals, what is the rule for selecting the primary one?
4. What is the acceptable match discrepancy threshold (% of loans or % of balance)?
5. Are there any loans in the Input Dataset that come from nCino only (not Core)?
6. Does the Core system have a `modification_date` or `extended_maturity_date` for modified loans?
