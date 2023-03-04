-- This query creates a single table with ongoing doses of vasoactive agents.
-- TBD: rarely angiotensin II, methylene blue, and
-- isoprenaline/isoproterenol are used. These are not in the query currently
-- as they are not documented in MetaVision. However, they could
-- be documented in other hospital wide systems.

-- collect all vasopressor administration times
-- create a single table with these as start/stop times

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.vasoactive_agent`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.vasoactive_agent` AS

WITH tm AS (
    SELECT
        icustay_id, starttime AS vasotime
    FROM `physionet-data.mimiciii_derived.dopamine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, starttime AS vasotime
    FROM `physionet-data.mimiciii_derived.epinephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, starttime AS vasotime
    FROM `physionet-data.mimiciii_derived.norepinephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, starttime AS vasotime
    FROM `physionet-data.mimiciii_derived.phenylephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, starttime AS vasotime
    FROM `physionet-data.mimiciii_derived.vasopressin_dose`
    UNION DISTINCT
    -- combine end times from the same tables
    SELECT
        icustay_id, endtime AS vasotime
    FROM `physionet-data.mimiciii_derived.dopamine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, endtime AS vasotime
    FROM `physionet-data.mimiciii_derived.epinephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, endtime AS vasotime
    FROM `physionet-data.mimiciii_derived.norepinephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, endtime AS vasotime
    FROM `physionet-data.mimiciii_derived.phenylephrine_dose`
    UNION DISTINCT
    SELECT
        icustay_id, endtime AS vasotime
    FROM `physionet-data.mimiciii_derived.vasopressin_dose`
)

-- create starttime/endtime from all possible times collected
, tm_lag AS (
    SELECT icustay_id
        , vasotime AS starttime
        -- note: the last row for each partition (icustay_id) will have
        -- a NULL endtime. we can drop this row later, as we know that no
        -- vasopressor will start at this time (otherwise, we would have
        -- a later end time, which would mean it's not the last row!)
        , LEAD(
            vasotime, 1
        ) OVER (PARTITION BY icustay_id ORDER BY vasotime) AS endtime
    FROM tm
)

-- left join to raw data tables to combine doses
SELECT t.icustay_id, t.starttime, t.endtime
    -- inopressors/vasopressors
    , dop.vaso_rate AS dopamine
    , epi.vaso_rate AS epinephrine
    , nor.vaso_rate AS norepinephrine
    , phe.vaso_rate AS phenylephrine
    , vas.vaso_rate AS vasopressin
-- isoproterenol is used in CCU/CVICU but not in metavision
-- other drugs not included here but (rarely) used in the BIDMC:
-- angiotensin II, methylene blue
FROM tm_lag t
LEFT JOIN `physionet-data.mimiciii_derived.dopamine_dose` dop
    ON t.icustay_id = dop.icustay_id
        AND t.starttime >= dop.starttime
        AND t.endtime <= dop.endtime
LEFT JOIN `physionet-data.mimiciii_derived.epinephrine_dose` epi
    ON t.icustay_id = epi.icustay_id
        AND t.starttime >= epi.starttime
        AND t.endtime <= epi.endtime
LEFT JOIN `physionet-data.mimiciii_derived.norepinephrine_dose` nor
    ON t.icustay_id = nor.icustay_id
        AND t.starttime >= nor.starttime
        AND t.endtime <= nor.endtime
LEFT JOIN `physionet-data.mimiciii_derived.phenylephrine_dose` phe
    ON t.icustay_id = phe.icustay_id
        AND t.starttime >= phe.starttime
        AND t.endtime <= phe.endtime
LEFT JOIN `physionet-data.mimiciii_derived.vasopressin_dose` vas
    ON t.icustay_id = vas.icustay_id
        AND t.starttime >= vas.starttime
        AND t.endtime <= vas.endtime
-- remove the final row for each icustay_id
-- it will not have any infusions associated with it
WHERE t.endtime IS NOT NULL;