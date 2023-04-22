-- This query calculates norepinephrine equivalent dose for vasopressors.
-- Based on "Vasopressor dose equivalence: A scoping review and
-- suggested formula" by Goradia et al. 2020.

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.norepinephrine_equivalent_dose`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.norepinephrine_equivalent_dose` AS


SELECT icustay_id, starttime, endtime
    -- calculate the dose
    , ROUND(CAST(
        COALESCE(norepinephrine, 0)
        + COALESCE(epinephrine, 0)
        + COALESCE(phenylephrine / 10, 0)
        + COALESCE(dopamine / 100, 0)
        -- + metaraminol/8 -- metaraminol not used in BIDMC
        + COALESCE(vasopressin * 2.5, 0)
        -- angiotensin_ii*10 -- angiotensin ii rarely used, though
        -- it could be included due to norepinephrine sparing effects
        AS NUMERIC), 4) AS norepinephrine_equivalent_dose
FROM `golden-rite-376204.mimiciii_pulseOx.vasoactive_agent`
WHERE norepinephrine IS NOT NULL
    OR epinephrine IS NOT NULL
    OR phenylephrine IS NOT NULL
    OR dopamine IS NOT NULL
    OR vasopressin IS NOT NULL;