-- Blood Values include hemoglobin, hematocrit, platelet, wbc. Missing: mch, mchc, mcv, rbc, rdw

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.blood_count`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.blood_count` AS

WITH
hematocrit AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, hematocrit.charttime, MINUTE) AS delta_hematocrit
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, hematocrit.charttime, MINUTE)) ASC ) AS seq
      , hematocrit.hematocrit

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` hematocrit

    ON hematocrit.icustay_id = pairs.icustay_id
    AND hematocrit.hematocrit IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, hemoglobin AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, hemoglobin.charttime, MINUTE) AS delta_hemoglobin
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, hemoglobin.charttime, MINUTE)) ASC ) AS seq
      , hemoglobin.hemoglobin

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` hemoglobin

    ON hemoglobin.icustay_id = pairs.icustay_id
    AND hemoglobin.hemoglobin IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, platelet AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, platelet.charttime, MINUTE) AS delta_platelet
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, platelet.charttime, MINUTE)) ASC ) AS seq
      , platelet.platelet

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` platelet

    ON platelet.subject_id = pairs.subject_id
    AND platelet.platelet IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, wbc AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, wbc.charttime, MINUTE) AS delta_wbc
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, wbc.charttime, MINUTE)) ASC ) AS seq
      , wbc.wbc

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` wbc

    ON wbc.subject_id = pairs.subject_id
    AND wbc.wbc IS NOT NULL
      
    ) 
    WHERE seq = 1
)

SELECT 
    pairs.subject_id
  , pairs.icustay_id
  , pairs.SaO2_timestamp
  , pairs.SaO2
  , pairs.delta_SpO2
  , pairs.SpO2
  , pairs.hidden_hypoxemia
  , hemoglobin.delta_hemoglobin
  , hemoglobin.hemoglobin
  , hematocrit.delta_hematocrit
  , hematocrit.hematocrit
  , platelet.delta_platelet
  , platelet.platelet
  , wbc.delta_wbc
  , wbc.wbc

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.icustay_id = pairs.icustay_id
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.icustay_id = pairs.icustay_id
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN platelet
ON platelet.subject_id = pairs.subject_id
AND platelet.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN wbc
ON wbc.subject_id = pairs.subject_id
AND wbc.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, icustay_id, SaO2_timestamp