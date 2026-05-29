# Challenger Model for Commercial NOOCRE Stress Testing — Construction Outline

---

## 1. Design Philosophy and Scope

The challenger's job is not to be as sophisticated as Compass — it is to be **independently calibrated, transparent, and directionally reliable** enough to give the MRM team a basis for evaluating whether Compass outputs are reasonable. The two key questions it must answer:

- Do both models agree on **which loans and segments are riskiest?** (rank-ordering)
- Do both models produce **portfolio EL estimates in a defensible range** under the same scenarios?

The challenger mirrors Compass's two-stage architecture — collateral projection first, credit risk second — but uses simpler methods and independent data sources at every step. This challenger is scoped to **Term Risk only** and does not implement a Refinance Risk module.

---

## 2. Required Inputs

The challenger should consume the **same loan-level data tape** submitted to CoStar, ensuring comparison is apples-to-apples. The minimum required fields are:

| Field | Use |
|---|---|
| Loan balance / outstanding balance | LTV projection, LGD |
| Original LTV at origination or most recent appraisal | Starting point for LTV projection |
| Original / current DSCR | Starting point for DSCR projection |
| Property type (Office, Retail, MF, Industrial, Hotel, etc.) | Scenario shocks, LGD lookup |
| Market / MSA / zip code | Macro shock assignment |
| Loan maturity date | Term horizon |
| Interest rate and amortization schedule | Payment projection |
| Origination / vintage date | Seasoning |

---

## 3. Module 1 — Challenger Collateral Stress Projection (Simplified DTR)

Compass uses a deeply proprietary interlinked regression system (vacancy → rent → NOI → cap rate → value) driven by CoStar's database. The challenger replaces this with **scenario-driven index shocks** applied to the loan's current NOI and appraised value, using publicly available macro and market data.

### 3a. Macroeconomic Scenario Inputs

Use the **Federal Reserve's DFAST supervisory scenarios** (published annually, publicly available) as the primary scenario driver. The key variables from the Fed scenarios that drive CRE performance are:

- GDP growth rate (quarterly path)
- Unemployment rate (quarterly path)
- 10-year Treasury yield (quarterly path)
- BBB corporate spread / Moody's Baa yield (cap rate driver)
- Residential and commercial real estate price indices (Fed publishes CRE price paths for Baseline and Severely Adverse)

This directly parallels how Compass constructs its "Fed Base" and "Fed Stress" scenarios, giving you an immediately comparable scenario basis.

### 3b. NOI Stress Index

Rather than modeling rent → vacancy → NOI from scratch, apply a **property-type and MSA-specific NOI growth index** derived from:

- **NCREIF Property Index (NPI)** — publicly available quarterly NOI and value data by property type and region. This is the most defensible independent source for NOI change factors.
- Under stress: apply the Fed's CRE price path as a proxy for NOI deterioration using a historically calibrated relationship between CRE price declines and NOI declines (e.g., from the GFC period: CRE prices fell ~40%, NOI fell ~20–25% — the ratio is empirically estimable).

The resulting NOI stress factor by property type and scenario quarter is:

$$\text{NOI Stress Factor}(t, \text{PropType}) = 1 + \Delta\text{NOI}_{t,\ \text{PropType}}$$

Applied as:

$$\text{NOI}_{stressed}(t) = \text{NOI}_{current} \times \prod_{s=1}^{t} \text{NOI Stress Factor}(s,\ \text{PropType})$$

### 3c. Cap Rate Stress Index

Cap rate is the key driver of value in CRE. Model a stressed cap rate as a function of the scenario's interest rate path, using a simple linear regression calibrated on historical data:

$$\hat{\text{CapRate}}(t) = \alpha + \beta_1 \cdot \text{Baa Yield}(t) + \beta_2 \cdot \text{Unemployment}(t)$$

This mirrors the Compass DTR cap rate model formula almost exactly (`CapRate_t = f(Baa_t, UR_t, ΔlogRent_t)`), simplified by dropping the rent change term. Calibrate $\alpha$, $\beta_1$, $\beta_2$ on NCREIF/CBRE cap rate data from 2000 to present by property type using OLS — this is straightforward with publicly available series from FRED and CBRE/Green Street research.

### 3d. Stressed Value Projection

$$\text{Value}_{stressed}(t) = \frac{\text{NOI}_{stressed}(t)}{\hat{\text{CapRate}}(t)}$$

This is the same fundamental CRE identity Compass uses (`Yield = NOI / Value` rearranged). No proprietary data is required.

### 3e. Stressed DSCR and LTV Projection

Apply the same projection formulas Compass uses (these are standard CRE math, not proprietary):

$$\text{DSCR}(t) = \frac{\text{NOI}_{stressed}(t)}{\text{Debt Service}(t)}$$

$$\text{LTV}(t) = \frac{\text{Outstanding Balance}(t)}{\text{Value}_{stressed}(t)}$$

Where debt service and outstanding balance are computed from the loan's amortization schedule — a deterministic calculation requiring only the rate, term, and amortization type from the data tape.

---

## 4. Module 2 — Challenger Term Credit Risk Model

This module takes the projected DSCR(t) and LTV(t) paths from Module 1 and converts them into quarterly PD, LGD, and EL estimates.

### 4a. PD Methodology Options — Choose One

Two viable approaches depending on team data access and capacity:

---

#### Option A: Empirical Default Rate Matrix *(Recommended for simplicity and defensibility)*

Build a lookup table of **historical annual default rates** by DSCR bucket and LTV bucket, calibrated on publicly available CMBS performance data. Trepp publishes CMBS delinquency and default rates; academic CMBS studies (e.g., Ciochetti et al., Ambrose & Sanders) provide loan-level default rates by risk stratum. The Federal Reserve's SR-letter datasets and FDIC bank failure data can supplement.

| DSCR \ LTV | LTV < 60% | 60–70% | 70–80% | 80–90% | LTV > 90% |
|---|---|---|---|---|---|
| DSCR > 1.50x | 0.1% | 0.2% | 0.4% | 0.8% | 1.5% |
| 1.25–1.50x | 0.3% | 0.5% | 1.0% | 2.0% | 3.5% |
| 1.10–1.25x | 0.8% | 1.5% | 2.5% | 4.0% | 6.5% |
| 1.00–1.10x | 2.0% | 3.5% | 5.5% | 8.0% | 12.0% |
| DSCR < 1.00x | 5.0% | 8.0% | 12.0% | 18.0% | 25.0% |

*(Illustrative — populate from calibration data)*

This produces an annual conditional default rate. Convert to quarterly:

$$CDR_q = 1 - (1 - CDR_{annual})^{0.25}$$

At each projected quarter $t$, look up the DSCR(t) / LTV(t) cell to get the period CDR. This is transparent, auditable, and directly parallels how Compass produces a CDR driven by DSCR and LTV.

---

#### Option B: Logistic Regression *(Closer structural parallel to Compass's white paper)*

Estimate a logistic regression where the log-odds of default is a function of projected DSCR and LTV, mirroring the Compass white paper's CDR framework:

$$\log\left(\frac{CDR(t)}{1 - CDR(t)}\right) = \alpha + \beta_1 \cdot \text{DSCR}(t) + \beta_2 \cdot \text{LTV}(t) + \beta_3 \cdot \text{LoanAge}(t) + \beta_4 \cdot \text{PropType}$$

Calibrate using CMBS loan performance data (Trepp, or academic datasets) or the bank's own loan-level historical loss experience if sufficient. The advantage of this approach is that it produces a PD curve (not a lookup table) and more closely mirrors Compass's model class — making comparison more direct and the discussion with MRM more precise.

---

### 4b. Survival Rate

Apply a simplified competing-risks survival rate to convert CDR to unconditional PD. The challenger can use a **property-type-specific empirical prepayment and paydown schedule** from CMBS studies rather than a dynamic model:

$$S(t) = S(t-1) \times \left[1 - CPDR(t) - CPR_{assumed} - CDR(t)\right]$$

Where:
- $CPDR(t)$ is computed deterministically from the amortization schedule
- $CPR_{assumed}$ is a fixed or scenario-conditioned conditional prepayment rate (e.g., 5–10% CPR for stabilized performing loans; lower under stress when refinancing is constrained)

$$\text{Term PD}(t) = S(t-1) \times CDR(t)$$

$$\text{Cumulative Term PD} = \sum_{t=1}^{T} \text{Term PD}(t)$$

### 4c. Term LGD

Use a **LTV-based LGD function** anchored on historical CMBS recovery rates by property type:

$$\text{LGD}(t) = \max\left(0,\ LTV(t) - (1 - \text{HairCut}_{\text{PropType}})\right) + \text{Carrying Costs}$$

Where:
- `HairCut_PropType` reflects the historical average discount on distressed CRE sales below appraised value by property type (e.g., 15–20% for office and retail under stress; 10% for industrial/MF). Calibrate from FDIC failed bank asset sales, CMBS special servicing resolution data, or the Federal Reserve's LISCC CRE loss severity assumptions.
- `Carrying Costs` accounts for time-to-resolution costs (legal, servicing, carrying): typically 5–10% of outstanding balance.

A simpler lookup version:

| LTV at Default | LGD (Office/Retail) | LGD (MF/Industrial) | LGD (Hotel) |
|---|---|---|---|
| < 60% | 5% | 3% | 8% |
| 60–75% | 15% | 10% | 20% |
| 75–90% | 30% | 22% | 35% |
| 90–100% | 45% | 35% | 50% |
| > 100% | 60%+ | 50%+ | 65%+ |

*(Calibrate from CMBS loss severity studies — RealPage, Trepp, FDIC)*

### 4d. Term EL

$$\text{Term EL}(t) = \text{Term PD}(t) \times \text{LGD}(t)$$

$$\text{Cumulative Term EL} = \sum_{t=1}^{T} \text{Term EL}(t)$$

---

## 5. Output Aggregation

The challenger is scoped to Term Risk only. At the loan level it produces:

| Output | Challenger | Compass (Term) | Difference |
|---|---|---|---|
| Cumulative Term PD | ✓ | ✓ | Δ |
| Term LGD | ✓ | ✓ | Δ |
| Cumulative Term EL ($) | ✓ | ✓ | Δ |

At the portfolio level, aggregate EL, weighted-average PD, and weighted-average LGD are computed by property type, geography, vintage, and maturity bucket. When comparing to Compass, restrict the comparison to Compass's **Term** outputs only, isolating the term component from the combined total that Compass also reports.

---

## 6. Calibration Strategy

| Component | Calibration Source |
|---|---|
| NOI stress factors | NCREIF NPI by property type (2000–present); FRED CRE price index |
| Cap rate model coefficients | CBRE/Green Street cap rate series + FRED Baa yield + BLS unemployment (OLS regression) |
| CDR matrix / logistic coefficients | Trepp CMBS delinquency/default data; published CMBS studies; FDIC bank CRE loss data |
| CPR (prepayment) | CMBS prepayment studies by vintage/scenario; assume low CPR under stress |
| LGD table | CMBS special servicing resolution data; FDIC failed-bank CRE asset loss rates; Fed LISCC LGD assumptions |

Calibration should target the **GFC period (2007–2010)** as the primary stress anchor, since it represents the most complete historical CRE cycle available. COVID (2020) and the 2022–2023 rate shock period are secondary calibration points, especially for office and retail.

---

## 7. Comparison Reporting Framework

This is where the challenger delivers value to MRM. The comparison report should include:

### 7a. Rank-Order Validation

- **Gini coefficient / AUC**: Do both models rank-order the same loans as highest risk? Compute concordance between Compass loan-level Term PD rankings and challenger rankings.
- **KS Statistic**: Distribution separation between the two PD distributions.
- **Scatter plot**: Loan-level Compass Term PD (x-axis) vs. Challenger Term PD (y-axis), colored by property type.

### 7b. Portfolio-Level Comparison

- Aggregate Term EL under each scenario (Base, Stress): absolute difference and percentage difference
- EL by property type: identify where models diverge most systematically (e.g., if the challenger is consistently higher for office, that is a meaningful signal)
- EL by maturity bucket: identify if divergence concentrates in near-term vs. long-dated maturities

### 7c. Sensitivity / Scenario Analysis

- Apply a common macro shock (e.g., +200bps cap rate expansion, −20% NOI) to both models and compare the response. Do both models show similar directional sensitivity?
- A model that is much less responsive to shocks — or much more — is a candidate for further scrutiny.

### 7d. Conservative / Liberal Assessment

- Determine whether the challenger is systematically more or less conservative than Compass and document the direction and magnitude of the gap.
- If Compass is consistently producing materially lower EL than the challenger, that is a flag for MRM — the bank should understand *why* and whether Compass's assumptions (e.g., its GBM normality assumption, its calibration vintage window ending Q4 2020) are driving a systematic understatement.

### 7e. Recommended Reporting Template

| Metric | Compass Term (Stress) | Challenger (Stress) | Δ Absolute | Δ % | Flag |
|---|---|---|---|---|---|
| Portfolio Term EL ($MM) | | | | | |
| Weighted Avg. Term PD (%) | | | | | |
| Weighted Avg. LGD (%) | | | | | |
| EL — Office ($MM) | | | | | |
| EL — Retail ($MM) | | | | | |
| EL — Multifamily ($MM) | | | | | |
| EL — Industrial ($MM) | | | | | |
| EL — Hotel ($MM) | | | | | |
| Rank-Order Concordance (Gini) | N/A | N/A | — | — | |

A threshold for flagging divergence (e.g., >25% difference in portfolio Term EL, or Gini < 0.60) should be defined in MRM policy and trigger deeper review or vendor inquiry.

---

## 8. Key Limitations to Document

Any challenger model comes with limitations the MRM team should explicitly disclose:

1. **Term Risk only** — The challenger does not model refinance/balloon risk. Comparison to Compass must be restricted to Compass's Term outputs only. Portfolio-level EL will not be directly comparable to Compass's Total EL.
2. **No submarket granularity** — The challenger uses property-type and MSA-level shocks, not CoStar's submarket-level data. This is the single largest simplification relative to Compass.
3. **Simplified NOI dynamics** — Compass models lease roll dynamics explicitly (2.5% quarterly rent reset for commercial; 25% for multifamily). The challenger applies an index shock, not a lease-level rent roll. This can understate NOI resilience in portfolios with long-dated, below-market leases.
4. **Static prepayment assumption** — A fixed CPR is less accurate than Compass's dynamic prepay model, particularly in rate environments where prepayment behavior changes significantly.
5. **Calibration data coverage** — CMBS-based calibration may not perfectly represent a bank's whole-loan portfolio (CMBS loans skew larger and more institutional). Supplement with bank-specific loss data where available.
6. **Weibull vs. logistic CDR uncertainty** — As identified in the analyst review of the Compass documentation, the operative CDR methodology in the version of Compass currently deployed is not fully confirmed. MRM should focus comparison at the output level (Term PD, Term EL) rather than intermediate CDR calculations until this is resolved with the vendor.

---

## 9. Implementation Path

| Phase | Activity | Estimated Effort |
|---|---|---|
| 1 | Gather and clean loan data tape; obtain Fed DFAST scenarios | 1–2 weeks |
| 2 | Build cap rate regression and NOI stress index from NCREIF/FRED | 1–2 weeks |
| 3 | Build DSCR/LTV projection engine (amortization schedules + stress factors) | 1–2 weeks |
| 4 | Build CDR matrix (Option A) or calibrate logistic regression (Option B) | 2–4 weeks |
| 5 | Build LGD table from CMBS resolution data | 1 week |
| 6 | Build comparison reporting output | 1–2 weeks |
| 7 | Document, validate, present to MRM | 2–3 weeks |
| **Total** | | **~9–15 weeks** |

---

## 10. Closing Note on Purpose

The core deliverable for MRM is not a "better" model — it is a **transparent, independently built reference point** that gives the team informed grounds to either validate Compass's Term Risk outputs or escalate specific divergences for vendor discussion. The challenger's simplicity is a feature: if a straightforward, publicly-calibrated model tells a materially different story than Compass, that gap demands explanation. Conversely, when the two models agree directionally under stress, that convergence meaningfully increases confidence in the vendor model's outputs and supports its continued use.

---

---

## 11. Data Scientist / Analyst Implementation Guide

This section translates the model outline above into a step-by-step technical workflow. All computations are expressed in SQL (PostgreSQL syntax; adaptable to Snowflake, BigQuery, or SQL Server with minor dialect changes). Python is recommended for regression calibration steps that fall outside SQL's native capabilities; those steps are noted where applicable.

---

### Step 1 — Environment and Tool Setup

**Recommended Stack:**

| Tool | Purpose |
|---|---|
| PostgreSQL / Snowflake | Primary computation and data warehouse engine |
| Python (pandas, statsmodels, scikit-learn) | Cap rate regression calibration, Gini/KS statistics |
| Git + DVC | Version control for data and model artifacts |
| Excel / CSV | Ingesting Fed DFAST scenario files and NCREIF downloads |

**Folder structure convention:**
```
challenger_model/
├── data/
│   ├── raw/          # unmodified source files
│   ├── staged/       # cleaned, loaded to DB
│   └── outputs/      # final loan-level and portfolio results
├── sql/              # all SQL scripts, numbered by step
├── python/           # regression calibration notebooks
└── reports/          # comparison output files
```

---

### Step 2 — Data Acquisition and Sources

Download and stage the following datasets before writing any model SQL. All links below are publicly accessible unless noted as requiring a subscription.

| Dataset | Source | Link | Format |
|---|---|---|---|
| DFAST Supervisory Scenarios (Baseline & Severely Adverse) | Federal Reserve | [https://www.federalreserve.gov/supervisionreg/stress-tests-capital-planning.htm](https://www.federalreserve.gov/supervisionreg/stress-tests-capital-planning.htm) | Excel (annual release) |
| OCC DFAST Scenarios | OCC | [https://www.occ.gov/topics/supervision-and-examination/capital-adequacy/stress-testing/index-stress-testing.html](https://www.occ.gov/topics/supervision-and-examination/capital-adequacy/stress-testing/index-stress-testing.html) | Excel |
| Moody's Baa Corporate Bond Yield | FRED | [https://fred.stlouisfed.org/series/BAA](https://fred.stlouisfed.org/series/BAA) | CSV via API or download |
| 10-Year Treasury Yield | FRED | [https://fred.stlouisfed.org/series/GS10](https://fred.stlouisfed.org/series/GS10) | CSV |
| Unemployment Rate | FRED / BLS | [https://fred.stlouisfed.org/series/UNRATE](https://fred.stlouisfed.org/series/UNRATE) | CSV |
| Commercial Real Estate Price Index | FRED | [https://fred.stlouisfed.org/series/COMREPUSQ159N](https://fred.stlouisfed.org/series/COMREPUSQ159N) | CSV (quarterly) |
| NCREIF Property Index (NPI) — NOI & Value by property type | NCREIF | [https://www.ncreif.org/data-products/ncreif-property-index/](https://www.ncreif.org/data-products/ncreif-property-index/) | Excel (member access; summary data free) |
| BLS Metro Employment Data | BLS | [https://www.bls.gov/data/](https://www.bls.gov/data/) | CSV / API |
| FDIC Failed Bank List & Loss Data | FDIC | [https://www.fdic.gov/bank/individual/failed/banklist.html](https://www.fdic.gov/bank/individual/failed/banklist.html) | CSV |
| FDIC Bank Statistics API | FDIC | [https://banks.data.fdic.gov/docs/](https://banks.data.fdic.gov/docs/) | JSON / CSV API |
| CBRE Cap Rate Survey | CBRE | [https://www.cbre.com/insights/figures/cap-rate-survey](https://www.cbre.com/insights/figures/cap-rate-survey) | Excel (free download) |
| Trepp CMBS Data (default, delinquency, loss severity) | Trepp | [https://www.trepp.com/](https://www.trepp.com/) | Subscription required |
| Green Street Cap Rate Research | Green Street | [https://www.greenstreet.com/](https://www.greenstreet.com/) | Subscription required |
| FFIEC Call Report Data (Schedule RC-C Part II — CRE) | FFIEC | [https://www.ffiec.gov/npw/FinancialReport/ReturnFinancialReport](https://www.ffiec.gov/npw/FinancialReport/ReturnFinancialReport) | CSV bulk download |
| CMBS Academic Studies (calibration reference) | SSRN / NY Fed | [https://www.newyorkfed.org/research](https://www.newyorkfed.org/research) | PDF / data supplement |
| RealPage CRE Analytics (multifamily) | RealPage | [https://www.realpage.com/analytics/](https://www.realpage.com/analytics/) | Subscription required |

> **Note on NCREIF:** The full NPI database requires NCREIF membership. However, aggregate returns by property type and region are published quarterly in NCREIF's public research section and in academic papers. For the cap rate calibration, CBRE's quarterly Cap Rate Survey (free) is a strong substitute or supplement.

---

### Step 3 — Database Schema Setup

Create the core tables that will hold all staged inputs and intermediate outputs.

```sql
-- ============================================================
-- STEP 3: Schema Setup
-- ============================================================

-- Loan data tape (populated from bank's submission file)
CREATE TABLE loan_tape (
    loan_id             VARCHAR(50) PRIMARY KEY,
    property_type       VARCHAR(30),    -- Office, Retail, MF, Industrial, Hotel, Mixed
    msa_code            VARCHAR(10),    -- MSA or CBSA code
    zip_code            VARCHAR(10),
    origination_date    DATE,
    maturity_date       DATE,
    original_balance    NUMERIC(18,2),
    current_balance     NUMERIC(18,2),
    current_dscr        NUMERIC(8,4),
    current_ltv         NUMERIC(8,4),   -- expressed as decimal (e.g., 0.75)
    current_noi         NUMERIC(18,2),
    appraised_value     NUMERIC(18,2),
    interest_rate       NUMERIC(8,6),   -- annual rate as decimal
    amort_term_months   INTEGER,        -- amortization term (months)
    loan_term_months    INTEGER,        -- contractual term (months)
    amort_type          VARCHAR(20),    -- 'fully_amortizing', 'interest_only', 'partial_io'
    io_period_months    INTEGER,        -- months of interest-only if partial_io
    vintage_year        INTEGER,
    as_of_date          DATE            -- date of DSCR/LTV observation
);

-- Macro scenarios (Fed DFAST — one row per scenario / quarter)
CREATE TABLE macro_scenarios (
    scenario_name       VARCHAR(30),    -- 'baseline', 'severely_adverse'
    scenario_quarter    DATE,           -- first day of each projected quarter
    quarter_num         INTEGER,        -- quarters from stress test start (1..N)
    gdp_growth          NUMERIC(8,4),   -- annualized quarterly GDP growth rate
    unemployment_rate   NUMERIC(8,4),   -- level
    ten_yr_treasury     NUMERIC(8,4),   -- 10-yr yield, decimal
    baa_yield           NUMERIC(8,4),   -- Moody's Baa yield, decimal
    cre_price_chg_qoq   NUMERIC(8,4),   -- quarterly % change in nat'l CRE price index
    PRIMARY KEY (scenario_name, scenario_quarter)
);

-- NCREIF / CBRE market index — NOI and cap rate history by property type
CREATE TABLE market_index (
    property_type       VARCHAR(30),
    index_quarter       DATE,
    noi_index           NUMERIC(12,4),  -- indexed to 100 at base period
    cap_rate            NUMERIC(8,6),   -- observed cap rate as decimal
    value_index         NUMERIC(12,4),
    PRIMARY KEY (property_type, index_quarter)
);

-- CDR default rate matrix (calibrated from CMBS / FDIC data)
CREATE TABLE cdr_matrix (
    dscr_bucket         VARCHAR(20),    -- e.g., 'gt_150', '125_150', '110_125', '100_110', 'lt_100'
    ltv_bucket          VARCHAR(20),    -- e.g., 'lt_60', '60_70', '70_80', '80_90', 'gt_90'
    annual_cdr          NUMERIC(8,6),   -- annual conditional default rate as decimal
    PRIMARY KEY (dscr_bucket, ltv_bucket)
);

-- LGD lookup table by property type and LTV range
CREATE TABLE lgd_matrix (
    property_type_group VARCHAR(20),    -- 'office_retail', 'mf_industrial', 'hotel'
    ltv_lower           NUMERIC(8,4),
    ltv_upper           NUMERIC(8,4),
    lgd_rate            NUMERIC(8,6),
    PRIMARY KEY (property_type_group, ltv_lower)
);

-- Cap rate regression coefficients by property type (populated from Python OLS)
CREATE TABLE caprate_coefficients (
    property_type       VARCHAR(30),
    intercept           NUMERIC(12,8),
    beta_baa            NUMERIC(12,8),
    beta_unemployment   NUMERIC(12,8),
    PRIMARY KEY (property_type)
);

-- Stress test run log
CREATE TABLE stress_run_log (
    run_id              SERIAL PRIMARY KEY,
    run_date            TIMESTAMP DEFAULT NOW(),
    scenario_name       VARCHAR(30),
    notes               TEXT
);
```

---

### Step 4 — Load Reference Tables

Populate the CDR matrix, LGD matrix, and cap rate coefficients. The CDR and LGD values below are illustrative; replace with calibrated values from your CMBS/FDIC analysis.

```sql
-- ============================================================
-- STEP 4: Load CDR Matrix (illustrative — calibrate from data)
-- ============================================================

INSERT INTO cdr_matrix (dscr_bucket, ltv_bucket, annual_cdr) VALUES
-- DSCR > 1.50x
('gt_150',  'lt_60',  0.0010),
('gt_150',  '60_70',  0.0020),
('gt_150',  '70_80',  0.0040),
('gt_150',  '80_90',  0.0080),
('gt_150',  'gt_90',  0.0150),
-- DSCR 1.25–1.50x
('125_150', 'lt_60',  0.0030),
('125_150', '60_70',  0.0050),
('125_150', '70_80',  0.0100),
('125_150', '80_90',  0.0200),
('125_150', 'gt_90',  0.0350),
-- DSCR 1.10–1.25x
('110_125', 'lt_60',  0.0080),
('110_125', '60_70',  0.0150),
('110_125', '70_80',  0.0250),
('110_125', '80_90',  0.0400),
('110_125', 'gt_90',  0.0650),
-- DSCR 1.00–1.10x
('100_110', 'lt_60',  0.0200),
('100_110', '60_70',  0.0350),
('100_110', '70_80',  0.0550),
('100_110', '80_90',  0.0800),
('100_110', 'gt_90',  0.1200),
-- DSCR < 1.00x
('lt_100',  'lt_60',  0.0500),
('lt_100',  '60_70',  0.0800),
('lt_100',  '70_80',  0.1200),
('lt_100',  '80_90',  0.1800),
('lt_100',  'gt_90',  0.2500);


-- ============================================================
-- Load LGD Matrix (illustrative — calibrate from CMBS/FDIC)
-- ============================================================

INSERT INTO lgd_matrix (property_type_group, ltv_lower, ltv_upper, lgd_rate) VALUES
('office_retail',  0.00, 0.60, 0.05),
('office_retail',  0.60, 0.75, 0.15),
('office_retail',  0.75, 0.90, 0.30),
('office_retail',  0.90, 1.00, 0.45),
('office_retail',  1.00, 9.99, 0.60),
('mf_industrial',  0.00, 0.60, 0.03),
('mf_industrial',  0.60, 0.75, 0.10),
('mf_industrial',  0.75, 0.90, 0.22),
('mf_industrial',  0.90, 1.00, 0.35),
('mf_industrial',  1.00, 9.99, 0.50),
('hotel',          0.00, 0.60, 0.08),
('hotel',          0.60, 0.75, 0.20),
('hotel',          0.75, 0.90, 0.35),
('hotel',          0.90, 1.00, 0.50),
('hotel',          1.00, 9.99, 0.65);


-- ============================================================
-- Load Cap Rate Coefficients (populate after Python OLS step)
-- ============================================================

INSERT INTO caprate_coefficients (property_type, intercept, beta_baa, beta_unemployment) VALUES
('Office',      0.0180,  0.4800,  0.0900),
('Retail',      0.0210,  0.5100,  0.0750),
('MF',          0.0150,  0.3900,  0.0600),
('Industrial',  0.0160,  0.4200,  0.0550),
('Hotel',       0.0290,  0.5500,  0.1100);
-- Replace with OLS output from Python calibration (see Step 5)
```

---

### Step 5 — Cap Rate Regression Calibration (Python + SQL)

Before running the SQL stress projection, calibrate the cap rate regression coefficients in Python using historical NCREIF/CBRE and FRED data.

**Python workflow (run once to produce coefficients):**

```python
# python/calibrate_caprate.py
import pandas as pd
import statsmodels.formula.api as smf
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:pass@localhost/challenger_db")

# Load historical market index data (NCREIF cap rates + FRED macro)
df = pd.read_sql("""
    SELECT m.property_type, m.index_quarter, m.cap_rate,
           s.baa_yield, s.unemployment_rate
    FROM market_index m
    JOIN macro_scenarios s
        ON s.scenario_quarter = m.index_quarter
        AND s.scenario_name = 'historical'
    WHERE m.index_quarter BETWEEN '2000-01-01' AND '2024-12-31'
""", engine)

results = []
for prop_type in df['property_type'].unique():
    sub = df[df['property_type'] == prop_type].dropna()
    model = smf.ols('cap_rate ~ baa_yield + unemployment_rate', data=sub).fit()
    results.append({
        'property_type':       prop_type,
        'intercept':           model.params['Intercept'],
        'beta_baa':            model.params['baa_yield'],
        'beta_unemployment':   model.params['unemployment_rate'],
        'r_squared':           model.rsquared
    })
    print(f"{prop_type}: R²={model.rsquared:.3f}")

coef_df = pd.DataFrame(results)
# Upsert coefficients back to DB
coef_df.to_sql('caprate_coefficients', engine, if_exists='replace', index=False)
print("Coefficients written to DB.")
```

After running the Python script, the `caprate_coefficients` table is populated and all subsequent steps run in SQL.

---

### Step 6 — Amortization Schedule Engine

Generate a quarterly amortization schedule for every loan through its maturity date. This produces the outstanding balance and debt service payment at each future quarter, which are inputs to the DSCR and LTV projections.

```sql
-- ============================================================
-- STEP 6: Amortization Schedule (Recursive CTE)
-- Compatible with PostgreSQL; for Snowflake use GENERATOR()
-- ============================================================

CREATE TABLE amort_schedule AS

WITH RECURSIVE quarters AS (
    -- Anchor: loan at origination / as-of date
    SELECT
        l.loan_id,
        l.property_type,
        l.msa_code,
        l.interest_rate,
        l.amort_term_months,
        l.loan_term_months,
        l.io_period_months,
        l.amort_type,
        l.maturity_date,
        l.current_balance                       AS beg_balance,
        0                                       AS quarter_num,
        l.as_of_date                            AS quarter_date,
        -- Quarterly interest-only payment
        l.current_balance * (l.interest_rate / 4) AS interest_payment,
        -- Quarterly principal (0 if IO, else standard amortization)
        CASE
            WHEN l.amort_type = 'interest_only' THEN 0
            WHEN l.amort_type = 'partial_io' AND 0 < l.io_period_months / 3
                THEN 0
            ELSE
                -- Standard mortgage payment formula (monthly, scaled to quarter)
                l.current_balance
                * (l.interest_rate / 12)
                / (1 - POWER(1 + l.interest_rate / 12, -l.amort_term_months))
                * 3  -- 3 monthly payments per quarter
                - l.current_balance * (l.interest_rate / 4)
        END                                     AS principal_payment
    FROM loan_tape l

    UNION ALL

    -- Recursive step: advance one quarter
    SELECT
        q.loan_id,
        q.property_type,
        q.msa_code,
        q.interest_rate,
        q.amort_term_months,
        q.loan_term_months,
        q.io_period_months,
        q.amort_type,
        q.maturity_date,
        -- New beginning balance = prior ending balance
        GREATEST(q.beg_balance - q.principal_payment, 0) AS beg_balance,
        q.quarter_num + 1,
        (q.quarter_date + INTERVAL '3 months')::DATE,
        -- Recompute interest on new balance
        GREATEST(q.beg_balance - q.principal_payment, 0)
            * (q.interest_rate / 4)             AS interest_payment,
        -- Recompute principal (handle IO period by quarter count)
        CASE
            WHEN q.amort_type = 'interest_only' THEN 0
            WHEN q.amort_type = 'partial_io'
                AND (q.quarter_num + 1) <= (q.io_period_months / 3) THEN 0
            ELSE
                GREATEST(q.beg_balance - q.principal_payment, 0)
                * (q.interest_rate / 12)
                / (1 - POWER(1 + q.interest_rate / 12,
                       -(q.amort_term_months - (q.quarter_num + 1) * 3)))
                * 3
                - GREATEST(q.beg_balance - q.principal_payment, 0)
                    * (q.interest_rate / 4)
        END                                     AS principal_payment
    FROM quarters q
    WHERE (q.quarter_date + INTERVAL '3 months')::DATE <= q.maturity_date
      AND q.beg_balance > 0
)

SELECT
    loan_id,
    property_type,
    msa_code,
    quarter_num,
    quarter_date,
    beg_balance                                         AS outstanding_balance,
    principal_payment,
    interest_payment,
    principal_payment + interest_payment                AS total_debt_service,
    -- CPDR: principal payment / beginning balance (competing risk component)
    CASE WHEN beg_balance > 0
         THEN principal_payment / beg_balance
         ELSE 0 END                                     AS cpdr
FROM quarters
ORDER BY loan_id, quarter_num;

-- Index for downstream join performance
CREATE INDEX idx_amort_loan_qtr ON amort_schedule (loan_id, quarter_num);
```

---

### Step 7 — Macro Scenario and NOI Stress Factor

Build a cumulative NOI stress factor for each property type and scenario quarter, derived from the Fed DFAST CRE price path and the historically calibrated NOI-to-price ratio.

```sql
-- ============================================================
-- STEP 7: NOI Stress Factor by Property Type and Quarter
-- ============================================================

-- 7a. Define the NOI-to-CRE-price sensitivity ratio by property type.
--     Calibrate from NCREIF data: regress quarterly NOI growth on
--     quarterly CRE price growth (2000–2024).
--     Illustrative ratios below; replace with calibrated values.

CREATE TABLE noi_price_sensitivity (
    property_type   VARCHAR(30) PRIMARY KEY,
    noi_beta        NUMERIC(8,4)  -- NOI growth = noi_beta * CRE_price_growth
);

INSERT INTO noi_price_sensitivity VALUES
('Office',      0.55),
('Retail',      0.50),
('MF',          0.60),
('Industrial',  0.65),
('Hotel',       0.70);  -- hotels have higher income volatility


-- 7b. Compute quarterly NOI stress factor per scenario and property type

CREATE TABLE noi_stress_factors AS
SELECT
    ms.scenario_name,
    ms.scenario_quarter,
    ms.quarter_num,
    nps.property_type,
    ms.baa_yield,
    ms.unemployment_rate,
    ms.cre_price_chg_qoq,
    -- Quarterly NOI growth rate = beta * CRE price quarterly change
    nps.noi_beta * ms.cre_price_chg_qoq                AS noi_growth_qoq,
    -- Cumulative NOI stress factor (product over all quarters up to t)
    -- Computed via window function using LOG trick for products
    EXP(
        SUM(LN(1 + nps.noi_beta * ms.cre_price_chg_qoq))
        OVER (
            PARTITION BY ms.scenario_name, nps.property_type
            ORDER BY ms.quarter_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )                                                   AS cumulative_noi_factor
FROM macro_scenarios ms
CROSS JOIN noi_price_sensitivity nps
WHERE ms.scenario_name IN ('baseline', 'severely_adverse')
ORDER BY ms.scenario_name, nps.property_type, ms.quarter_num;

CREATE INDEX idx_noi_stress ON noi_stress_factors (scenario_name, property_type, quarter_num);
```

---

### Step 8 — Stressed Value and Projected DSCR / LTV

Join the amortization schedule, NOI stress factors, and cap rate regression to produce stressed DSCR and LTV at every quarter for every loan under each scenario.

```sql
-- ============================================================
-- STEP 8: Collateral Stress Projection — Stressed DSCR and LTV
-- ============================================================

CREATE TABLE stressed_projections AS
SELECT
    a.loan_id,
    a.property_type,
    a.quarter_num,
    a.quarter_date,
    a.outstanding_balance,
    a.total_debt_service,
    a.cpdr,
    nsf.scenario_name,

    -- Stressed NOI
    l.current_noi * nsf.cumulative_noi_factor           AS noi_stressed,

    -- Stressed Cap Rate: alpha + beta_baa * Baa + beta_ur * UR
    cc.intercept
        + cc.beta_baa          * ms.baa_yield
        + cc.beta_unemployment * ms.unemployment_rate   AS cap_rate_stressed,

    -- Stressed Property Value = NOI_stressed / CapRate_stressed
    CASE
        WHEN (cc.intercept
                + cc.beta_baa * ms.baa_yield
                + cc.beta_unemployment * ms.unemployment_rate) > 0
        THEN (l.current_noi * nsf.cumulative_noi_factor)
             / (cc.intercept
                + cc.beta_baa * ms.baa_yield
                + cc.beta_unemployment * ms.unemployment_rate)
        ELSE NULL
    END                                                 AS value_stressed,

    -- Projected DSCR = NOI_stressed / Debt Service
    CASE
        WHEN a.total_debt_service > 0
        THEN (l.current_noi * nsf.cumulative_noi_factor)
             / a.total_debt_service
        ELSE NULL
    END                                                 AS dscr_projected,

    -- Projected LTV = Outstanding Balance / Value_stressed
    CASE
        WHEN (cc.intercept
                + cc.beta_baa * ms.baa_yield
                + cc.beta_unemployment * ms.unemployment_rate) > 0
             AND (l.current_noi * nsf.cumulative_noi_factor) > 0
        THEN a.outstanding_balance
             / ((l.current_noi * nsf.cumulative_noi_factor)
                / (cc.intercept
                   + cc.beta_baa * ms.baa_yield
                   + cc.beta_unemployment * ms.unemployment_rate))
        ELSE NULL
    END                                                 AS ltv_projected

FROM amort_schedule a
JOIN loan_tape l
    ON a.loan_id = l.loan_id
JOIN noi_stress_factors nsf
    ON nsf.property_type  = a.property_type
    AND nsf.quarter_num   = a.quarter_num
JOIN macro_scenarios ms
    ON ms.scenario_name   = nsf.scenario_name
    AND ms.quarter_num    = nsf.quarter_num
JOIN caprate_coefficients cc
    ON cc.property_type   = a.property_type
ORDER BY a.loan_id, nsf.scenario_name, a.quarter_num;

CREATE INDEX idx_proj_loan_scen_qtr ON stressed_projections (loan_id, scenario_name, quarter_num);
```

---

### Step 9 — DSCR and LTV Bucketing

Assign each projected DSCR and LTV to its CDR matrix bucket for the lookup in the next step.

```sql
-- ============================================================
-- STEP 9: DSCR and LTV Bucket Assignment
-- ============================================================

CREATE TABLE projections_bucketed AS
SELECT
    sp.*,

    -- DSCR bucket
    CASE
        WHEN sp.dscr_projected >  1.50 THEN 'gt_150'
        WHEN sp.dscr_projected >  1.25 THEN '125_150'
        WHEN sp.dscr_projected >  1.10 THEN '110_125'
        WHEN sp.dscr_projected >= 1.00 THEN '100_110'
        ELSE                                 'lt_100'
    END AS dscr_bucket,

    -- LTV bucket
    CASE
        WHEN sp.ltv_projected <  0.60 THEN 'lt_60'
        WHEN sp.ltv_projected <  0.70 THEN '60_70'
        WHEN sp.ltv_projected <  0.80 THEN '70_80'
        WHEN sp.ltv_projected <  0.90 THEN '80_90'
        ELSE                               'gt_90'
    END AS ltv_bucket,

    -- LGD property type grouping
    CASE
        WHEN sp.property_type IN ('Office', 'Retail')       THEN 'office_retail'
        WHEN sp.property_type IN ('MF', 'Industrial')       THEN 'mf_industrial'
        WHEN sp.property_type = 'Hotel'                     THEN 'hotel'
        ELSE                                                     'office_retail'  -- default
    END AS lgd_property_group

FROM stressed_projections sp
WHERE sp.dscr_projected IS NOT NULL
  AND sp.ltv_projected  IS NOT NULL;

CREATE INDEX idx_bucketed ON projections_bucketed (loan_id, scenario_name, quarter_num);
```

---

### Step 10 — CDR Calculation

Look up the annual CDR from the matrix and convert to a quarterly rate.

```sql
-- ============================================================
-- STEP 10: Conditional Default Rate (CDR) per Quarter
-- ============================================================

CREATE TABLE cdr_by_quarter AS
SELECT
    pb.loan_id,
    pb.scenario_name,
    pb.quarter_num,
    pb.quarter_date,
    pb.property_type,
    pb.outstanding_balance,
    pb.total_debt_service,
    pb.cpdr,
    pb.dscr_projected,
    pb.ltv_projected,
    pb.dscr_bucket,
    pb.ltv_bucket,
    pb.lgd_property_group,
    pb.value_stressed,

    -- Annual CDR from matrix lookup
    cm.annual_cdr,

    -- Convert annual CDR to quarterly CDR
    -- CDR_q = 1 - (1 - CDR_annual)^0.25
    1.0 - POWER(1.0 - cm.annual_cdr, 0.25)             AS cdr_quarterly,

    -- Assumed quarterly CPR (stress: 2%; base: 5%)
    -- Override with loan-level CPR if available
    CASE
        WHEN pb.scenario_name = 'severely_adverse' THEN 0.02
        ELSE 0.05
    END                                                 AS cpr_assumed

FROM projections_bucketed pb
JOIN cdr_matrix cm
    ON cm.dscr_bucket = pb.dscr_bucket
    AND cm.ltv_bucket = pb.ltv_bucket;

CREATE INDEX idx_cdr ON cdr_by_quarter (loan_id, scenario_name, quarter_num);
```

---

### Step 11 — Survival Rate Computation

Compute the survival rate at each quarter using a window-based cumulative product. This is the probability that a loan is still outstanding (not yet paid off, prepaid, or defaulted) at the beginning of each period.

```sql
-- ============================================================
-- STEP 11: Survival Rate (Competing Risks)
-- S(t) = S(t-1) * [1 - CPDR(t) - CPR(t) - CDR(t)]
-- Uses LOG/EXP window trick for cumulative product in SQL
-- ============================================================

CREATE TABLE survival_rates AS
SELECT
    c.loan_id,
    c.scenario_name,
    c.quarter_num,
    c.quarter_date,
    c.outstanding_balance,
    c.total_debt_service,
    c.cdr_quarterly,
    c.cpr_assumed,
    c.cpdr,
    c.dscr_projected,
    c.ltv_projected,
    c.value_stressed,
    c.lgd_property_group,

    -- Degradation factor for this quarter
    GREATEST(
        1.0 - c.cpdr - c.cpr_assumed - c.cdr_quarterly,
        0.0
    )                                                   AS degradation_factor,

    -- S(t-1): survival rate at beginning of this quarter
    -- Computed as cumulative product of prior quarters' degradation factors
    CASE
        WHEN c.quarter_num = 0 THEN 1.0
        ELSE
            EXP(
                SUM(
                    LN(GREATEST(
                        1.0 - c.cpdr - c.cpr_assumed - c.cdr_quarterly,
                        0.000001  -- floor to avoid LOG(0)
                    ))
                ) OVER (
                    PARTITION BY c.loan_id, c.scenario_name
                    ORDER BY c.quarter_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
            )
    END                                                 AS survival_rate_prior

FROM cdr_by_quarter c;

CREATE INDEX idx_surv ON survival_rates (loan_id, scenario_name, quarter_num);
```

---

### Step 12 — Term PD Calculation

Compute the unconditional quarterly Term PD and cumulative Term PD for each loan.

```sql
-- ============================================================
-- STEP 12: Term PD
-- Term PD(t) = S(t-1) * CDR(t)
-- Cumulative Term PD = SUM over all quarters
-- ============================================================

CREATE TABLE term_pd AS
SELECT
    sr.loan_id,
    sr.scenario_name,
    sr.quarter_num,
    sr.quarter_date,
    sr.dscr_projected,
    sr.ltv_projected,
    sr.outstanding_balance,
    sr.survival_rate_prior,
    sr.cdr_quarterly,
    sr.lgd_property_group,
    sr.value_stressed,

    -- Unconditional quarterly Term PD
    sr.survival_rate_prior * sr.cdr_quarterly           AS term_pd_quarterly,

    -- Cumulative Term PD through quarter t
    SUM(sr.survival_rate_prior * sr.cdr_quarterly)
        OVER (
            PARTITION BY sr.loan_id, sr.scenario_name
            ORDER BY sr.quarter_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                               AS term_pd_cumulative

FROM survival_rates sr;

CREATE INDEX idx_tpd ON term_pd (loan_id, scenario_name, quarter_num);
```

---

### Step 13 — LGD Estimation

Look up the LGD for each loan-quarter based on the projected LTV at potential default and property type group.

```sql
-- ============================================================
-- STEP 13: LGD Lookup
-- ============================================================

CREATE TABLE term_lgd AS
SELECT
    tp.loan_id,
    tp.scenario_name,
    tp.quarter_num,
    tp.quarter_date,
    tp.outstanding_balance,
    tp.ltv_projected,
    tp.lgd_property_group,
    tp.term_pd_quarterly,
    tp.term_pd_cumulative,

    -- LGD from lookup table based on projected LTV at time of potential default
    lm.lgd_rate                                         AS lgd

FROM term_pd tp
JOIN lgd_matrix lm
    ON lm.property_type_group = tp.lgd_property_group
    AND tp.ltv_projected >= lm.ltv_lower
    AND tp.ltv_projected  < lm.ltv_upper;

CREATE INDEX idx_lgd ON term_lgd (loan_id, scenario_name, quarter_num);
```

---

### Step 14 — Term EL Calculation

Compute quarterly and cumulative Term EL.

```sql
-- ============================================================
-- STEP 14: Term Expected Loss
-- Term EL(t) = Term PD(t) * LGD(t)
-- Dollar EL uses outstanding balance as EAD
-- ============================================================

CREATE TABLE term_el AS
SELECT
    tl.loan_id,
    tl.scenario_name,
    tl.quarter_num,
    tl.quarter_date,
    tl.outstanding_balance,
    tl.ltv_projected,
    tl.lgd,
    tl.term_pd_quarterly,
    tl.term_pd_cumulative,

    -- Rate-based quarterly EL
    tl.term_pd_quarterly * tl.lgd                       AS term_el_quarterly,

    -- Dollar EL (EAD = outstanding balance)
    tl.outstanding_balance
        * tl.term_pd_quarterly
        * tl.lgd                                        AS term_el_quarterly_dollars,

    -- Cumulative EL rate through quarter t
    SUM(tl.term_pd_quarterly * tl.lgd)
        OVER (
            PARTITION BY tl.loan_id, tl.scenario_name
            ORDER BY tl.quarter_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                               AS term_el_cumulative,

    -- Cumulative dollar EL
    SUM(tl.outstanding_balance * tl.term_pd_quarterly * tl.lgd)
        OVER (
            PARTITION BY tl.loan_id, tl.scenario_name
            ORDER BY tl.quarter_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                               AS term_el_cumulative_dollars

FROM term_lgd tl;

CREATE INDEX idx_el ON term_el (loan_id, scenario_name, quarter_num);
```

---

### Step 15 — Loan-Level Summary Output

Collapse the quarterly detail to a single-row-per-loan summary for reporting and comparison with Compass.

```sql
-- ============================================================
-- STEP 15: Loan-Level Summary
-- ============================================================

CREATE TABLE loan_level_summary AS
SELECT
    te.loan_id,
    l.property_type,
    l.msa_code,
    l.vintage_year,
    l.maturity_date,
    l.current_balance,
    l.current_dscr,
    l.current_ltv,
    te.scenario_name,

    -- Final-quarter cumulative metrics
    MAX(te.term_pd_cumulative)                          AS cumulative_term_pd,
    MAX(te.term_el_cumulative)                          AS cumulative_term_el_rate,
    MAX(te.term_el_cumulative_dollars)                  AS cumulative_term_el_dollars,

    -- Weighted-average LGD over the loan life (weighted by quarterly PD)
    SUM(te.term_pd_quarterly * te.lgd)
        / NULLIF(MAX(te.term_pd_cumulative), 0)         AS wtd_avg_lgd,

    -- Peak stressed DSCR trough and LTV peak
    MIN(te.ltv_projected)                               AS min_projected_dscr,  -- placeholder
    MAX(te.ltv_projected)                               AS max_projected_ltv,

    COUNT(te.quarter_num)                               AS quarters_remaining

FROM term_el te
JOIN loan_tape l ON l.loan_id = te.loan_id
GROUP BY
    te.loan_id, l.property_type, l.msa_code, l.vintage_year,
    l.maturity_date, l.current_balance, l.current_dscr,
    l.current_ltv, te.scenario_name;

CREATE INDEX idx_summary ON loan_level_summary (loan_id, scenario_name);
```

---

### Step 16 — Portfolio Aggregation

Roll up loan-level results to portfolio segments for reporting.

```sql
-- ============================================================
-- STEP 16: Portfolio Aggregation
-- ============================================================

-- 16a. By property type
SELECT
    scenario_name,
    property_type,
    COUNT(loan_id)                                      AS loan_count,
    SUM(current_balance)                                AS total_exposure,
    -- Weighted average Term PD (weighted by balance)
    SUM(cumulative_term_pd * current_balance)
        / NULLIF(SUM(current_balance), 0)               AS wtd_avg_term_pd,
    -- Weighted average LGD
    SUM(wtd_avg_lgd * current_balance)
        / NULLIF(SUM(current_balance), 0)               AS wtd_avg_lgd,
    SUM(cumulative_term_el_dollars)                     AS total_term_el_dollars,
    -- EL as % of portfolio exposure
    SUM(cumulative_term_el_dollars)
        / NULLIF(SUM(current_balance), 0)               AS el_rate
FROM loan_level_summary
GROUP BY scenario_name, property_type
ORDER BY scenario_name, total_term_el_dollars DESC;


-- 16b. By MSA / geography
SELECT
    scenario_name,
    msa_code,
    COUNT(loan_id)                                      AS loan_count,
    SUM(current_balance)                                AS total_exposure,
    SUM(cumulative_term_el_dollars)                     AS total_term_el_dollars,
    SUM(cumulative_term_el_dollars)
        / NULLIF(SUM(current_balance), 0)               AS el_rate
FROM loan_level_summary
GROUP BY scenario_name, msa_code
ORDER BY scenario_name, total_term_el_dollars DESC;


-- 16c. By maturity bucket (years to maturity)
SELECT
    scenario_name,
    CASE
        WHEN DATE_PART('year', maturity_date - CURRENT_DATE) <= 1 THEN '0-1yr'
        WHEN DATE_PART('year', maturity_date - CURRENT_DATE) <= 3 THEN '1-3yr'
        WHEN DATE_PART('year', maturity_date - CURRENT_DATE) <= 5 THEN '3-5yr'
        ELSE '5yr+'
    END                                                 AS maturity_bucket,
    COUNT(loan_id)                                      AS loan_count,
    SUM(current_balance)                                AS total_exposure,
    SUM(cumulative_term_el_dollars)                     AS total_term_el_dollars
FROM loan_level_summary
GROUP BY scenario_name, maturity_bucket
ORDER BY scenario_name, maturity_bucket;


-- 16d. By vintage year
SELECT
    scenario_name,
    vintage_year,
    COUNT(loan_id)                                      AS loan_count,
    SUM(current_balance)                                AS total_exposure,
    SUM(cumulative_term_el_dollars)                     AS total_term_el_dollars,
    SUM(cumulative_term_el_dollars)
        / NULLIF(SUM(current_balance), 0)               AS el_rate
FROM loan_level_summary
GROUP BY scenario_name, vintage_year
ORDER BY scenario_name, vintage_year;
```

---

### Step 17 — Compass Comparison Output

Stage the Compass loan-level Term outputs received from the vendor and compute the comparison delta table used in the MRM report.

```sql
-- ============================================================
-- STEP 17: Compass Comparison
-- ============================================================

-- 17a. Compass output staging table
--      (populated from the Compass output file delivered by CoStar)
CREATE TABLE compass_outputs (
    loan_id             VARCHAR(50),
    scenario_name       VARCHAR(30),
    compass_term_pd     NUMERIC(10,6),
    compass_term_lgd    NUMERIC(10,6),
    compass_term_el     NUMERIC(18,2),
    PRIMARY KEY (loan_id, scenario_name)
);


-- 17b. Loan-level comparison
CREATE TABLE comparison_loan_level AS
SELECT
    lls.loan_id,
    lls.property_type,
    lls.msa_code,
    lls.current_balance,
    lls.current_dscr,
    lls.current_ltv,
    lls.scenario_name,

    -- Challenger outputs
    lls.cumulative_term_pd                              AS challenger_term_pd,
    lls.wtd_avg_lgd                                     AS challenger_lgd,
    lls.cumulative_term_el_dollars                      AS challenger_term_el,

    -- Compass outputs
    co.compass_term_pd,
    co.compass_term_lgd,
    co.compass_term_el,

    -- Absolute differences
    lls.cumulative_term_pd - co.compass_term_pd         AS delta_pd,
    lls.wtd_avg_lgd        - co.compass_term_lgd        AS delta_lgd,
    lls.cumulative_term_el_dollars - co.compass_term_el AS delta_el_dollars,

    -- Relative differences (challenger vs. Compass)
    CASE WHEN co.compass_term_pd > 0
         THEN (lls.cumulative_term_pd - co.compass_term_pd) / co.compass_term_pd
         ELSE NULL END                                  AS pct_diff_pd,

    -- Direction flag: does challenger agree on risk direction?
    CASE
        WHEN lls.cumulative_term_pd > co.compass_term_pd THEN 'Challenger Higher'
        WHEN lls.cumulative_term_pd < co.compass_term_pd THEN 'Compass Higher'
        ELSE 'Equal'
    END                                                 AS pd_direction

FROM loan_level_summary lls
LEFT JOIN compass_outputs co
    ON co.loan_id      = lls.loan_id
    AND co.scenario_name = lls.scenario_name;


-- 17c. Portfolio-level comparison summary
SELECT
    c.scenario_name,
    c.property_type,
    COUNT(c.loan_id)                                    AS loan_count,
    SUM(c.current_balance)                              AS total_exposure,

    -- Challenger
    SUM(c.current_balance * c.challenger_term_pd)
        / NULLIF(SUM(c.current_balance), 0)             AS challenger_wtd_pd,
    SUM(c.challenger_term_el)                           AS challenger_total_el,

    -- Compass
    SUM(c.current_balance * c.compass_term_pd)
        / NULLIF(SUM(c.current_balance), 0)             AS compass_wtd_pd,
    SUM(c.compass_term_el)                              AS compass_total_el,

    -- Delta
    SUM(c.challenger_term_el) - SUM(c.compass_term_el) AS delta_el_dollars,
    (SUM(c.challenger_term_el) - SUM(c.compass_term_el))
        / NULLIF(SUM(c.compass_term_el), 0)             AS delta_el_pct,

    -- Agreement ratio: % of loans where both models agree challenger >= compass
    AVG(CASE WHEN c.pd_direction = 'Challenger Higher' THEN 1.0 ELSE 0.0 END)
                                                        AS pct_challenger_higher

FROM comparison_loan_level c
GROUP BY c.scenario_name, c.property_type
ORDER BY c.scenario_name, challenger_total_el DESC;
```

---

### Step 18 — Rank-Order Validation (Gini / KS)

Compute the Gini coefficient and KS statistic to assess rank-order concordance between the challenger and Compass. This is best done in Python after pulling results from the database, but a SQL approximation of rank correlation is shown below.

```sql
-- ============================================================
-- STEP 18: Rank-Order Concordance (SQL approximation)
-- Full Gini / AUC / KS are best computed in Python (sklearn)
-- ============================================================

-- 18a. Spearman rank correlation between challenger and Compass Term PD
--      Uses the standard formula: 1 - 6*SUM(d²) / (n*(n²-1))

WITH ranked AS (
    SELECT
        loan_id,
        scenario_name,
        RANK() OVER (PARTITION BY scenario_name ORDER BY challenger_term_pd DESC)
                                                        AS rank_challenger,
        RANK() OVER (PARTITION BY scenario_name ORDER BY compass_term_pd DESC)
                                                        AS rank_compass,
        COUNT(*) OVER (PARTITION BY scenario_name)      AS n
    FROM comparison_loan_level
    WHERE challenger_term_pd IS NOT NULL
      AND compass_term_pd    IS NOT NULL
),
diffs AS (
    SELECT
        scenario_name,
        n,
        POWER(rank_challenger - rank_compass, 2) AS d_squared
    FROM ranked
)
SELECT
    scenario_name,
    COUNT(*)                                            AS n,
    1.0 - (6.0 * SUM(d_squared))
          / NULLIF(COUNT(*) * (POWER(COUNT(*), 2) - 1), 0)
                                                        AS spearman_rank_correlation
FROM diffs
GROUP BY scenario_name;


-- 18b. Flag loans with large absolute PD divergence (>10pp) for review
SELECT
    loan_id,
    property_type,
    msa_code,
    current_balance,
    scenario_name,
    challenger_term_pd,
    compass_term_pd,
    delta_pd,
    pct_diff_pd,
    pd_direction
FROM comparison_loan_level
WHERE ABS(delta_pd) > 0.10       -- flagging >10 percentage point divergence
   OR ABS(pct_diff_pd) > 0.50    -- or >50% relative divergence
ORDER BY ABS(delta_pd) DESC;
```

**Python complement for full Gini / AUC (run after Step 18 SQL):**

```python
# python/validation_stats.py
import pandas as pd
from sklearn.metrics import roc_auc_score
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:pass@localhost/challenger_db")

df = pd.read_sql("""
    SELECT challenger_term_pd, compass_term_pd
    FROM comparison_loan_level
    WHERE scenario_name = 'severely_adverse'
      AND challenger_term_pd IS NOT NULL
      AND compass_term_pd IS NOT NULL
""", engine)

# Treat Compass as the "true" ranking baseline
# Binarize: top quartile of Compass PD = "high risk" = 1
threshold = df['compass_term_pd'].quantile(0.75)
df['high_risk_actual'] = (df['compass_term_pd'] >= threshold).astype(int)

auc = roc_auc_score(df['high_risk_actual'], df['challenger_term_pd'])
gini = 2 * auc - 1

print(f"AUC:  {auc:.4f}")
print(f"Gini: {gini:.4f}")
# Gini > 0.40 = acceptable; > 0.60 = strong concordance
```

---

### Step 19 — Final Checklist Before MRM Submission

| Item | SQL / Action | Status |
|---|---|---|
| All loans in tape have amortization schedules | `SELECT COUNT(DISTINCT loan_id) FROM amort_schedule` | |
| No NULL DSCR or LTV projections in first 4 quarters | `WHERE quarter_num <= 4 AND dscr_projected IS NULL` | |
| CDR matrix covers all DSCR/LTV bucket combinations | Check for unmatched rows in `cdr_by_quarter` | |
| Survival rate stays between 0 and 1 at all quarters | `WHERE survival_rate_prior NOT BETWEEN 0 AND 1` | |
| Term PD never exceeds 1.0 cumulatively | `WHERE term_pd_cumulative > 1.0` | |
| Challenger and Compass run on same scenario | Confirm `scenario_name` match in comparison table | |
| Portfolio EL within expected range vs. GFC benchmarks | Compare to FDIC/CMBS historical loss rates | |
| Spearman rank correlation ≥ 0.40 | From Step 18 output | |
| Divergence flags reviewed and documented | From Step 18 loan-level flag query | |
| Model limitations section included in report | See Section 8 above | |
