# CRE Stress Test — dbt Project

## Overview

This dbt project produces the **CRE Non-Owner Occupied Input Dataset** for the third-party vendor stress-test model. It replaces the manual data wrangling process previously used by the commercial lending team.

**Source systems:** Core (core banking) + nCino (Salesforce lending platform)
**Data warehouse:** Snowflake (read-only access) → pulled to local DuckDB
**Target period:** 12-31-2025 (primary), 3-31-2025 (secondary validation)

---

## Setup

### Prerequisites

```bash
pip install dbt-duckdb
```

### Configure profiles.yml

Copy `profiles.yml` from this repo to `~/.dbt/profiles.yml` and update the `path` to your local DuckDB file:

```yaml
cre_stress_test:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/your/snowflake_pull.duckdb
      threads: 4
```

### Run the project

```bash
cd dbt_project
dbt deps          # install packages (if any)
dbt run           # build all models
dbt test          # run all tests
dbt docs generate # generate documentation site
dbt docs serve    # open docs in browser
```

---

## Model DAG

```
sources (DuckDB raw tables)
  ├── stg_core_loans
  ├── stg_core_collaterals
  └── stg_ncino_loans
        │
        ▼
  int_cre_noo_loans          ← CRE NOO filter + ARC classification
        │
        ▼
  int_loan_collateral_match  ← Collateral join + address validation
        │
        ├── fct_cre_noo_stress_test_input   ← Vendor INPUT DATASET
        └── fct_data_quality_summary        ← PowerBI dashboard source
```

---

## Key Business Rules (to verify with team)

| Rule | Location | Status |
|---|---|---|
| CRE NOO filter field | `int_cre_noo_loans.sql` | **CONFIRM with team** |
| ARC loan definition | `int_cre_noo_loans.sql` | **CONFIRM with team** |
| Primary collateral selection | `int_loan_collateral_match.sql` | **CONFIRM with team** |
| Acceptable match discrepancy | `tests/assert_collateral_match_rate.sql` | Currently set to ≥98% |
| Amortization clock for IO loans | `macros/calculate_remaining_terms.sql` | Starts at IO end date |

---

## Validation

After running `dbt run`, compare to the manual 12-31-2025 Input Dataset:

1. Load the manual dataset into DuckDB as `input_dataset_manual_20251231`
2. Run `dbt test --select assert_input_dataset_parity`
3. Investigate any discrepancies
