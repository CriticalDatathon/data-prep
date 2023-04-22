-- SOFA scores from MIMIC -> see logic at 
-- https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iii/concepts/pivot/pivoted_sofa.sql
-- SOFA for coag, liver, cardiovasuclar, cns, and renal component only, no resp due to non existence in mimic-iv
-- paired on icustay_id -> other tables paired by subject_id
-- no charttime in derived-sofa table -> used endtime instead


DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.sofa`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.sofa` AS

WITH 

  sofa_coag AS (
  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , coagulation AS sofa_coag
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_coag
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_sofa` 
    AS sofa
    ON sofa.icustay_id = pairs.icustay_id
    AND coagulation IS NOT NULL
  ) 
  WHERE seq = 1

)

, sofa_liver AS (
  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , liver AS sofa_liver
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_liver
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_sofa` 
    AS sofa
    ON sofa.icustay_id = pairs.icustay_id
    AND liver IS NOT NULL
  ) 
  WHERE seq = 1

)

, sofa_cv AS (
  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , cardiovascular AS sofa_cv
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_cv
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_sofa` 
    AS sofa
    ON sofa.icustay_id = pairs.icustay_id
    AND cardiovascular IS NOT NULL

  ) 
  WHERE seq = 1

)

, sofa_cns AS (
  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , cns AS sofa_cns
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_cns
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_sofa` 
    AS sofa
    ON sofa.icustay_id = pairs.icustay_id
    AND cns IS NOT NULL

  ) 
  WHERE seq = 1

)

, sofa_renal AS (
  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , renal AS sofa_renal
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_renal
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_sofa` 
    AS sofa
    ON sofa.icustay_id = pairs.icustay_id
    AND renal IS NOT NULL

  ) 
  WHERE seq = 1

)



SELECT 
    pairs.subject_id
  , pairs.icustay_id
  , pairs.SaO2_timestamp
  , sofa_coag.delta_sofa_coag
  , sofa_coag.sofa_coag
  , sofa_liver.delta_sofa_liver
  , sofa_liver.sofa_liver
  , sofa_cv.delta_sofa_cv
  , sofa_cv.sofa_cv
  , sofa_cns.delta_sofa_cns
  , sofa_cns.sofa_cns
  , sofa_renal.delta_sofa_renal
  , sofa_renal.sofa_renal

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN sofa_coag
ON sofa_coag.icustay_id = pairs.icustay_id
AND sofa_coag.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_liver
ON sofa_liver.icustay_id = pairs.icustay_id
AND sofa_liver.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_cv
ON sofa_cv.icustay_id = pairs.icustay_id
AND sofa_cv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_cns
ON sofa_cns.icustay_id = pairs.icustay_id
AND sofa_cns.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_renal
ON sofa_renal.icustay_id = pairs.icustay_id
AND sofa_renal.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY pairs.subject_id, pairs.icustay_id, pairs.SaO2_timestamp