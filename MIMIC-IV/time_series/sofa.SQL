-- SOFA scores from MIMIC -> see logic at 
-- https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts/score/sofa.sql
-- SOFA for coag, liver, cardiovasuclar, cns, and renal component only, no resp due to outcome leakage
-- paired on stay_id -> other tables paired by subject_id
-- no charttime in derived-sofa table -> used endtime instead


DROP TABLE IF EXISTS `protean-chassis-368116.mimiciv_pulseOx.sofa`;
CREATE TABLE `protean-chassis-368116.mimiciv_pulseOx.sofa` AS

WITH 

  sofa_coag AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , coagulation AS sofa_coag
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_coag
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND coagulation IS NOT NULL
  ) 
  WHERE seq = 1

)

, sofa_liver AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , liver AS sofa_liver
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_liver
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND liver IS NOT NULL
  ) 
  WHERE seq = 1

)

, sofa_cv AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , cardiovascular AS sofa_cv
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_cv
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND cardiovascular IS NOT NULL

  ) 
  WHERE seq = 1

)

, sofa_cns AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , cns AS sofa_cns
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_cns
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND cns IS NOT NULL

  ) 
  WHERE seq = 1

)

, sofa_renal AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , renal AS sofa_renal
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_renal
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND renal IS NOT NULL

  ) 
  WHERE seq = 1

)

, sofa_resp AS (
  SELECT * FROM(
    SELECT
      pairs.stay_id
    , respiration AS sofa_resp
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(sofa.endtime, pairs.SaO2_timestamp, MINUTE) AS delta_sofa_resp
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sofa.endtime, MINUTE)) ASC) AS seq

    FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.sofa` 
    AS sofa
    ON sofa.stay_id = pairs.stay_id
    AND respiration IS NOT NULL

  ) 
  WHERE seq = 1

)



SELECT 
    pairs.subject_id
  , pairs.stay_id
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
  , sofa_resp.delta_sofa_resp
  , sofa_resp.sofa_resp

FROM `protean-chassis-368116.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN sofa_coag
ON sofa_coag.stay_id = pairs.stay_id
AND sofa_coag.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_liver
ON sofa_liver.stay_id = pairs.stay_id
AND sofa_liver.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_cv
ON sofa_cv.stay_id = pairs.stay_id
AND sofa_cv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_cns
ON sofa_cns.stay_id = pairs.stay_id
AND sofa_cns.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_renal
ON sofa_renal.stay_id = pairs.stay_id
AND sofa_renal.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sofa_resp
ON sofa_resp.stay_id = pairs.stay_id
AND sofa_resp.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY pairs.subject_id, pairs.stay_id, pairs.SaO2_timestamp
