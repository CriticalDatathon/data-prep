-- Vital signs include heart rate, blood pressure, respiration rate, temperature, glucose. Missing: rhythm

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.vital_signs`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.vital_signs` AS

WITH

hr AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, hr.charttime, MINUTE) AS delta_heart_rate
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, hr.charttime, MINUTE)) ASC ) AS seq
      , hr.heartrate as heart_rate

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_vital` hr

    ON hr.icustay_id = pairs.icustay_id
    AND hr.heartrate IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, rr AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, rr.charttime, MINUTE) AS delta_resp_rate
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rr.charttime, MINUTE)) ASC ) AS seq
      , rr.resprate as resp_rate

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_vital` rr

    ON rr.icustay_id = pairs.icustay_id
    AND rr.resprate IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, mbp AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , mbp.charttime AS mbp_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, mbp.charttime, MINUTE) AS delta_mbp
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, mbp.charttime, MINUTE)) ASC ) AS seq
      , mbp.meanbp as mbp

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_vital` mbp

    ON mbp.icustay_id = pairs.icustay_id
    AND mbp.meanbp IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, tmp AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, tmp.charttime, MINUTE) AS delta_temperature
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, tmp.charttime, MINUTE)) ASC ) AS seq
      , tmp.TempC as temperature

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_vital` tmp

    ON tmp.icustay_id = pairs.icustay_id
    AND tmp.TempC IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, glc AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, glc.charttime, MINUTE) AS delta_glucose
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, glc.charttime, MINUTE)) ASC ) AS seq
      , glc.glucose

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_vital` glc

    ON glc.icustay_id = pairs.icustay_id
    AND glc.glucose IS NOT NULL
      
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
  , rr.delta_resp_rate
  , rr.resp_rate
  , tmp.delta_temperature
  , tmp.temperature
  , glc.delta_glucose
  , glc.glucose

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN rr
ON rr.icustay_id = pairs.icustay_id
AND rr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN tmp
ON tmp.icustay_id = pairs.icustay_id
AND tmp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glc
ON glc.icustay_id = pairs.icustay_id
AND glc.SaO2_timestamp = pairs.SaO2_timestamp
