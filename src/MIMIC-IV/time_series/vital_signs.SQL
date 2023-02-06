-- Vital signs include heart rate, blood pressure, respiration rate, temperature, glucose, AND rhythm

DROP TABLE IF EXISTS `golden-rite-376204.mimiciv_pulseOx.vital_signs`;
CREATE TABLE `golden-rite-376204.mimiciv_pulseOx.vital_signs` AS

WITH 
  
  hr AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(hr.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_heart_rate
      , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, hr.charttime, MINUTE)) ASC ) AS seq
      , hr.heart_rate

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` hr

    ON hr.stay_id = pairs.stay_id
    AND hr.heart_rate IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, rr AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(rr.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_resp_rate
      , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rr.charttime, MINUTE)) ASC ) AS seq
      , rr.resp_rate

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` rr

    ON rr.stay_id = pairs.stay_id
    AND rr.resp_rate IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, mbp AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , mbp.charttime AS mbp_timestamp
      , TIMESTAMP_DIFF( mbp.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_mbp
      , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, mbp.charttime, MINUTE)) ASC ) AS seq
      , mbp.mbp

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` mbp

    ON mbp.stay_id = pairs.stay_id
    AND mbp.mbp IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, tmp AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(tmp.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_temperature
      , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, tmp.charttime, MINUTE)) ASC ) AS seq
      , tmp.temperature

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` tmp

    ON tmp.stay_id = pairs.stay_id
    AND tmp.temperature IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, glc AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(glc.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_glucose
      , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, glc.charttime, MINUTE)) ASC ) AS seq
      , glc.glucose

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` glc

    ON glc.stay_id = pairs.stay_id
    AND glc.glucose IS NOT NULL
      
    ) 
    WHERE seq = 1
) 

, rhythm AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , heart_rhythm
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(rh.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_heart_rhythm
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rh.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.rhythm` 
    AS rh
    ON rh.subject_id = pairs.subject_id
    AND heart_rhythm IS NOT NULL

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , hr.delta_heart_rate
  , hr.heart_rate
  , mbp.delta_mbp
  , mbp.mbp
  , rr.delta_resp_rate
  , rr.resp_rate
  , tmp.delta_temperature
  , tmp.temperature
  , glc.delta_glucose
  , glc.glucose
  , rhythm.delta_heart_rhythm
  , rhythm.heart_rhythm

FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hr
ON hr.stay_id = pairs.stay_id
AND hr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rr
ON rr.stay_id = pairs.stay_id
AND rr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mbp
ON mbp.stay_id = pairs.stay_id
AND mbp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN tmp
ON tmp.stay_id = pairs.stay_id
AND tmp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glc
ON glc.stay_id = pairs.stay_id
AND glc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rhythm
ON rhythm.subject_id = pairs.subject_id
AND rhythm.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp
