# Analyst Review — Term Risk Module

**Reviewer Role:** Portfolio Credit Risk Management Analyst (Model Risk Management)
**Review Date:** April 29, 2026
**Section Under Review:** Term Risk Module (`model_documentation.md`)
**Reference Documents:**
- `model_background.md` — Vendor model owner background excerpt
- `white_paper.md` — Relevant sections from the vendor's official model white paper

---

## Review Overview

This document reproduces the full **Term Risk Module** section from `model_documentation.md` with inline analyst comments. All comments and additions are clearly marked using the following convention:

> **[ANALYST COMMENT]** — flags content that is inaccurate, incomplete, ambiguous, or potentially misleading.

> **[ANALYST ADDITION]** — adds information deemed necessary for completeness and accuracy, with justification provided.

Text without any analyst markup is reproduced verbatim from the source documentation and is considered acceptable as written.

---

## Term Risk Module

### Term Probability of Default

The Compass PD model relies on a Merton type structural risk approach where default risk is primarily driven by the Z Distance to Default. The PD calculation uses a z-distance-to-default adjustment with a standard deviation based scale that measures the statistical likelihood a default will occur given a loan's then-current risk parameters, namely DSCR and LTV ratio, but also vintage of loan and amortization. Compass uses a multivariate modeling framework in which various risk factors are analyzed and weighted simultaneously. The model calculates an updated DSCR and LTV ratio at each period in time throughout the term of a loan's life under various macroeconomic scenarios. The calculations reflect CoStar's forecasted NOI and property value of the underlying collateral. The key risk parameters, DSCR and LTV, are converted into z-score statistical metrics (z-distance to default), which are then used in conjunction with calibrated transformation functions, baseline seasoning curves, and survival rates to facilitate the calculation of PD.

> **[ANALYST COMMENT — Risk Factor Enumeration]:** The opening paragraph identifies "vintage of loan and amortization" as additional risk factors beyond DSCR and LTV. This is partially correct but incomplete. Per Section 7.1 (Eq. 13) of the vendor white paper, the "Other" risk factor term in the CDR logistic regression also includes **loan age, loan size, region, building rating, and hotel class**. Vintage and amortization structure are relevant inputs but they are not an exhaustive list. The documentation should either enumerate all risk factors consistent with the white paper or describe them more broadly (e.g., "loan characteristics including loan age, size, amortization, and categorical property attributes"). The current language risks creating an incomplete picture for downstream users and reviewers.

---

"For a particular loan at each quarter during the projected period, two key risk drivers, DSCR and LTV, are projected based on reported loan DSCR and LTV as well as CoStar historical indices and forecasts for NOI and property price. The timing of the reported DSCR and LTV may be as of the loan origination date or updated anytime between origination and the current date."

$$
\text{DSCR}_{projected} = \frac{\text{NOI Index}_{projected}}{\text{NOI Index}_{original}} \times \frac{\text{Payment}_{original}}{\text{Payment}_{projected}} \times \text{DSCR}_{original}
$$

$$
\text{LTV}_{projected} = \frac{\text{Price Index}_{original}}{\text{Price Index}_{projected}} \times \frac{\text{Outstanding Balance}_{projected}}{\text{Outstanding Balance}_{original}} \times \text{LTV}_{original}
$$

> **[ANALYST COMMENT — Formula Notation Alignment]:** The DSCR and LTV projection formulas above use subscript notation "original" and "projected." The vendor white paper (Eq. 9 and Eq. 10) uses date-specific notation — `NOI Index(DateDSCR)`, `Price Index(DateLTV)`, etc. — to make explicit that the reference date for DSCR inputs and LTV inputs can differ (each may be as of origination or a subsequent re-appraisal/re-underwriting date). While mathematically equivalent, the documentation's use of "original" as a blanket label obscures this flexibility and could be misleading to a reviewer who assumes the reference date is always origination. It is recommended to adopt notation consistent with the white paper, or at minimum add a note clarifying that the reference date for DSCR and LTV inputs may differ and can be any date between origination and the current stress test date.

---

Under the Merton model, value (Price) and NOI are subject to a geometric Brownian motion distribution in which changes in value and NOI are assumed to be normally distributed. When asked, CoStar advised that "this provides a transparent and well-understood baseline and assures non-negative values. While empirical evidence shows that asset values and NOI can exhibit fat tails and extreme outcomes during periods of market stress, fully parametric models introduce additional complexity, calibration challenges and governance risk. In practice, tail risk is addressed through stress testing and scenario overlays rather than relying solely on distributional assumptions."

> **[ANALYST COMMENT — Distributional Assumption Limitation]:** CoStar's justification of the GBM/normal distribution assumption is reasonable and consistent with the white paper (Section 5.3.1.2). However, for Model Risk Management purposes, this section should explicitly acknowledge the known limitation: the normality assumption may systematically underestimate tail default risk during severe stress episodes (e.g., GFC 2008–2009, COVID-19). The statement that "tail risk is addressed through stress testing and scenario overlays" is a qualitative assertion with no further elaboration in this section. The documentation should cross-reference how the Bank's selected macroeconomic scenarios (e.g., CoStar Severe Recession, Fed Stress) operationalize this tail-risk mitigation, or acknowledge this as a model limitation requiring compensating controls.

---

"Compass's quarterly PD is defined as unconditional default and is calculated as the product of survival rate and conditional default rate (CDR) as shown in equation below. Survival rate is the probability of a loan surviving at the beginning of each time point, which measures the time-varying default exposure during that period. The inverse $(1 - \text{Survival rate})$ measures the probability of loan termination due to scheduled pay-down, prepayment, and default."

$$
\text{PD}(t) = \text{Survival}(t - 1) \times \text{CDR}(t)
$$

> **[ANALYST COMMENT — Missing Survival Rate Formula]:** The documentation defines survival rate conceptually but does not provide its calculation formula. This is a material omission. The survival rate is a core component of the PD calculation, not merely supporting context. Per the vendor white paper (Section 5.3.1.1 and Section 7.1), the survival rate at each quarter degrades from the prior period based on three competing-risk events: scheduled principal pay-down (`CPDR`), conditional prepayment (`CPR`), and the conditional default rate (`Term CDR`). The white paper provides two equivalent formulations (Eq. 1 and Eq. 2 using a Kaplan-Meier-type estimator; and the model implementation version in Section 7.1). The absence of the survival rate formula here requires a reviewer to consult external documents to understand a fundamental building block of PD. This should be remediated. See **[ANALYST ADDITION — Survival Rate]** below.

---

"Under the condition that the loan is outstanding at the beginning of a given time point, CDR measures the probability of default during that period. In Compass, CDR consists of two parts as shown in equation below - the baseline CDR, which only depends on loan seasoning (age) and reflects the loan's default pattern over its term; and the z-multiplier, which is a scalar applied to the baseline CDR to differentiate loans with varying specific risk characteristics (DSCR, LTV, etc.). In the PD calculation, the z-multiplier is translated from a z-distance to default through a calibrated Weibull distribution curve." (See pages 11 through 12 of the attached PDF Compass Basics for CoStar's justification of the Weibull distribution.)

$$
\text{CDR}(t) = \text{Baseline CDR}(t) \times Z\text{-multiplier}(t)
$$

> **[ANALYST COMMENT — CDR Formula: Weibull vs. Logistic Discrepancy — SIGNIFICANT FINDING]:** The CDR description and formula above present a material inconsistency with the vendor's own white paper (Section 5.3.1.3 and Section 7.1). This is the most significant finding in this review.
>
> **What the documentation states:** CDR is a multiplicative product of a "baseline CDR" (seasoning only) and a "z-multiplier" (DSCR/LTV adjustment), with the z-multiplier derived through a "calibrated Weibull distribution curve." The supporting citation is "Compass Basics" PDF (pages 11–12).
>
> **What the white paper states:** Per Section 5.3.1.3 ("Multivariate-Regression Based CDR") and Section 7.1 (Eq. 13), the Term CDR is computed using a **logistic regression model framework**, not a Weibull distribution. The full CDR formula from the white paper is:
>
> $$\text{Term CDR}(t) = \frac{\exp[\text{Term PD } Z_{DSCR}(t) \cdot a_1 + \text{Term PD } Z_{LTV}(t) \cdot a_2 + \text{other} \cdot a_3]}{1 + \exp[\text{Term PD } Z_{DSCR}(t) \cdot a_1 + \text{Term PD } Z_{LTV}(t) \cdot a_2 + \text{other} \cdot a_3]}
> $$
>
> The white paper makes no reference to a Baseline CDR × Z-multiplier decomposition or to Weibull distributions in the CDR calculation. Weibull distributions are a parametric survival model approach, while the white paper describes a logistic distribution function for the CDR — these are mathematically distinct.
>
> **Implication:** The documentation appears to describe an **older version** of the Compass model (as reflected in "Compass Basics"), while the white paper reflects the current model architecture. If the Bank is running the current version of Compass (which the white paper describes), the documentation's CDR description is outdated and inaccurate. This represents a documentation gap that understates the model's complexity and creates ambiguity about which version of the CDR methodology is actually in use.
>
> **Required action:** The Bank should confirm with CoStar which CDR methodology — Weibull-based multiplicative or logistic regression — is implemented in the version of Compass currently in use. If the logistic regression framework is operative, the CDR description must be updated accordingly (see **[ANALYST ADDITION — CDR Formula]** below). If the Weibull-based Baseline CDR × Z-multiplier approach is still in use for the term module, documentation to that effect (and reconciliation with the white paper) should be obtained from the vendor.

---

"Equations below show the formulas for the z-distance-to-default calculations. Two z-distances to default (also referred to as z-scores) are first calculated from DSCR and LTV separately. $\text{W}_{\text{NOI}}$ is the weight used in the z-distance-to-default calculation for PD. The z-distance to default for PD is then transformed to a z-multiplier."

$$
\text{Z distance to default}_{DSCR} = \frac{DSCR - 1}{DSCR \times \sigma_{NOI}}
$$

$$
\text{Z distance to default}_{LTV} = \frac{1 - LTV}{\sigma_{Value}}
$$

$$
\text{Z distance to default}_{PD} = \text{W}_{NOI} \times \text{Z distance to default}_{DSCR} + (1 - \text{W}_{NOI}) \times \text{Z distance to default}_{LTV}
$$

> **[ANALYST COMMENT — Z-Distance Formulas: Acceptable but Incomplete]:** The three Z-distance-to-default formulas above are consistent with the vendor white paper (Section 7.1, Eq. 11 and Eq. 12) and are correctly specified. The individual DSCR and LTV z-scores measure how many standard deviations each metric sits above its respective default threshold (1.0x for DSCR; 100% for LTV). However, two items require clarification:
>
> 1. **W_NOI weight**: The weight $\text{W}_{NOI}$ is introduced without explanation of how it is determined, calibrated, or whether it is property-type-specific. If the documentation is meant to stand independently, the source and value(s) of this parameter should be disclosed or cross-referenced. A reviewer cannot assess the appropriateness of the weighting without this information.
>
> 2. **Combined Z-score terminology**: The final line states the combined Z-score "is then transformed to a z-multiplier." Given the **[ANALYST COMMENT]** on the CDR formula above regarding the Weibull vs. logistic discrepancy, the phrase "transformed to a z-multiplier" is not reconcilable with the logistic regression framework in the white paper, where $Z_{DSCR}$ and $Z_{LTV}$ enter the CDR equation directly with regression coefficients $a_1$ and $a_2$ (not as a single combined z-score fed into a Weibull function). Under the white paper framework, the combined Z-score in the documentation may not exist as a distinct computed object. This further underscores the need to resolve the CDR methodology discrepancy identified above.

---

Volatility is a key input to the Z Distance to Default calculation. To estimate volatility by property type and market/submarket, the model computes the quarterly standard deviation of property level NOI/value growth then averages the standard deviations over a long period (Q1 2000 - Q4 2020).

> **[ANALYST COMMENT — Volatility Calibration Window: Potentially Outdated]:** The volatility estimation window of Q1 2000 – Q4 2020 should be flagged as potentially stale. As of the current stress test cycle (2026), this window is now five or more years behind the current period and excludes the post-COVID normalization period (2021–2023), the sharp interest rate rising cycle of 2022–2023, and ongoing NOOCRE valuation stress in certain property types (particularly office). These are periods of elevated CRE volatility that may be material to calibration. If CoStar has updated the volatility estimation window in the current model version, this documentation should reflect that. If the window has not been updated, this should be documented as a known limitation and flagged as a model monitoring item, as an outdated calibration window could cause the model to understate volatility — and therefore overstate z-scores — during stress scenarios resembling recent market conditions.

---

## Analyst Additions

The following additions are proposed for inclusion in the Term Risk Module to address material omissions identified above. Each addition is accompanied by a justification.

---

### [ANALYST ADDITION — Survival Rate Formula]

**Proposed addition — insert after the** $\text{PD}(t) = \text{Survival}(t-1) \times \text{CDR}(t)$ **formula:**

---

The survival rate $S(t)$ at each quarter degrades from the prior period based on three competing loan termination events: scheduled principal pay-down, conditional prepayment, and conditional default. In the model implementation, this is expressed as:

$$
S(t) = S(t-1) \times \left[1 - CPDR(t) - CPR(t) - \text{Term CDR}(t)\right]
$$

where:

- $CPDR(t)$ — Conditional principal down-payment ratio: the ratio of scheduled principal payment to the beginning outstanding balance in quarter $t$
- $CPR(t)$ — Conditional prepayment rate: loan-level forward-looking prepayment probability, derived from the Compass prepay model or user-defined inputs
- $\text{Term CDR}(t)$ — Conditional default rate: the probability of default in quarter $t$, given survival through $t-1$

All three events are treated as mutually exclusive competing risks. The survival rate begins at 100% at loan origination and decreases cumulatively over the loan's remaining term. All loans share $S(0) = 1.0$.

---

**Justification:** The survival rate formula is a foundational element of the Term PD calculation — it is as important as the CDR formula — yet it is entirely absent from the current documentation. A reader relying solely on this document cannot reconstruct the PD calculation without it. The formula above is sourced directly from the vendor white paper (Section 5.3.1.1 and Section 7.1, Eq. 2 and the S(t) implementation formula). In the context of Model Risk Management review, missing formulas constitute a documentation deficiency under standard MRM frameworks (e.g., SR 11-7), which require that model documentation be sufficiently complete for a knowledgeable party to understand and independently replicate key calculations. This addition directly remediates that gap.

---

### [ANALYST ADDITION — CDR Full Logistic Regression Formula (Conditional on Methodology Confirmation)]

**Proposed addition — to replace or supplement the** $\text{CDR}(t) = \text{Baseline CDR}(t) \times Z\text{-multiplier}(t)$ **formula, pending vendor confirmation of methodology version:**

---

*Note: The following formula reflects the CDR methodology described in the vendor's current white paper (Section 7.1). The Bank should confirm with CoStar that this formulation is operative in the Compass version currently deployed before finalizing this documentation.*

Per the vendor white paper, the Term Conditional Default Rate is computed using a logistic regression framework in which the Z-Distance-to-Default for DSCR and LTV, along with additional loan characteristics, enter directly as explanatory variables:

$$
\text{Term CDR}(t) = \frac{\exp\left[\text{Term PD } Z_{DSCR}(t) \cdot a_1 + \text{Term PD } Z_{LTV}(t) \cdot a_2 + \text{Other}(t) \cdot a_3\right]}{1 + \exp\left[\text{Term PD } Z_{DSCR}(t) \cdot a_1 + \text{Term PD } Z_{LTV}(t) \cdot a_2 + \text{Other}(t) \cdot a_3\right]}
$$

where:
- $a_1$, $a_2$, $a_3$ are regression coefficients estimated on CMBS loan performance data
- "Other" represents additional risk factors: loan age, loan size (categorical), region (categorical), building rating (for office/industrial properties), and hotel class (for hotel properties)

The logistic functional form guarantees that the CDR is bounded between 0 and 1 (ensuring valid probability outputs) and is monotonically increasing in the risk drivers. The S-shaped curve reflects the empirical observation that risk sensitivity is greatest near the default trigger zone and diminishes in the extreme-safe or extreme-distressed ranges.

---

**Justification:** If the logistic regression CDR is the operative methodology, the current documentation's Baseline CDR × Z-multiplier formulation and Weibull distribution reference are incorrect and must be replaced. The logistic model is more complex but also more accurate: it allows multiple risk factors to drive CDR simultaneously with calibrated regression weights, rather than relying on a single combined z-score scalar. This addition brings the documentation into alignment with the white paper, makes the model's actual behavior transparent to MRM reviewers, and ensures that the Bank's independent validation team is reviewing the correct methodology. It also discloses the full list of risk factors — which the current text incompletely represents as "vintage of loan and amortization" — improving completeness.

---

### [ANALYST ADDITION — Missing Term LGD and Term EL Documentation]

**Proposed addition — insert as a new subsection immediately after the Term PD discussion:**

---

**Term Loss Given Default (LGD) and Expected Loss (EL)**

The Term Risk Module produces not only a Term PD but also Term LGD and Term EL for each loan at each quarter of its remaining life. As documented in the Credit Risk Models section:

$$
\text{Term EL}(t) = \text{Term PD}(t) \times \text{Term LGD}(t)
$$

The aggregate Term LGD and Term EL, along with their Refinance counterparts, roll up to the total loan-level outputs:

$$
\text{Total PD} = \text{Term PD} + \text{Refi PD}
$$
$$
\text{Total LGD} = \text{Term LGD} + \text{Refi LGD}
$$
$$
\text{Total EL} = \text{Term EL} + \text{Refi EL}
$$

The Term LGD calculation methodology, inputs, and formulas should be documented here in the same level of detail as the Term PD section. [**Pending vendor documentation to be incorporated.**]

---

**Justification:** The current Term Risk Module section documents only the Term PD calculation in detail. However, Term LGD is equally important for stress testing purposes — the Bank's primary use of this model is NOOCRE portfolio stress testing, which requires EL outputs, not just PD. The model_background.md explicitly states that "DSC and LTV ratios drive PD, LGD, and EL calculations at the individual loan level" (Step 2b of the two-stage process). The aggregate formulas showing Term + Refi = Total are already present in the Credit Risk Models preamble of the documentation, but no description of *how* Term LGD is calculated exists anywhere in the Term Risk Module section. This is a material documentation gap under SR 11-7 standards, as the loss severity component of expected loss is as important as default probability for capital and CECL/stress test applications. The Bank should request the Term LGD methodology documentation from CoStar and incorporate it here.

---

## Summary of Findings

| # | Type | Severity | Description |
|---|------|----------|-------------|
| 1 | Comment | **High** | CDR formula uses Weibull/multiplicative framework inconsistent with white paper's logistic regression (Section 7.1). Version discrepancy must be resolved with vendor. |
| 2 | Addition | **High** | Term LGD and Term EL methodology is entirely absent from this section. Must be added for documentation completeness. |
| 3 | Addition | **Medium** | Survival rate formula S(t) is missing from the documentation despite being a required component of the PD calculation. |
| 4 | Comment | **Medium** | Risk factor enumeration for CDR is incomplete — omits region, loan size, building rating, hotel class per white paper. |
| 5 | Comment | **Medium** | Volatility calibration window (Q1 2000 – Q4 2020) is potentially outdated; does not capture 2021–2025 CRE market conditions. |
| 6 | Comment | **Medium** | W_NOI weight is undefined — its source, value, and whether it is property-type-specific is not disclosed. |
| 7 | Comment | **Low** | "Original" subscript notation in DSCR/LTV projection formulas obscures that reference dates for DSCR and LTV inputs may differ; notation should align with white paper. |
| 8 | Comment | **Low** | Distributional assumption limitation (GBM/normality) is acknowledged qualitatively but is not cross-referenced to compensating controls or specific stress scenarios. |
