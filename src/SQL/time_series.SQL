-- This queries relevant vital signs AND blood values 
-- based on: https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts/measurement/vitalsign.sql
-- Vital signs include heart rate, blood pressure, respiration rate, temperature, AND glucose
-- Blood Values include hemoglobin, hematocrit, mch, mchc, mcv, platelet, rbc, rdw, wbc
-- Metabolic Values include albumin, aniongap, bicarbonate, bun, calcium, chloride, glucose (lab), sodium, potassium

DROP TABLE IF EXISTS `golden-rite-376204.mimiciv_pulseOx.time_series`;
CREATE TABLE `golden-rite-376204.mimiciv_pulseOx.time_series` AS

WITH 
  hemoglobin AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , hemoglobin
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_hemoglobin
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND hemoglobin IS NOT NULL

  ) 
  WHERE seq = 1

)

, hematocrit AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , hematocrit
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_hematocrit
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND hematocrit IS NOT NULL

  ) 
  WHERE seq = 1

)

, mch AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mch
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_mch
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mch IS NOT NULL

  ) 
  WHERE seq = 1

)

, mchc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mchc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_mchc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mchc IS NOT NULL

  ) 
  WHERE seq = 1

)

, mcv AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mcv
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_mcv
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mcv IS NOT NULL

  ) 
  WHERE seq = 1

)

, platelet AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , platelet
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_platelet
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND platelet IS NOT NULL

  ) 
  WHERE seq = 1

)

, rbc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , rbc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_rbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND rbc IS NOT NULL

  ) 
  WHERE seq = 1

)

, rdw AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , rdw
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_rdw
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND rdw IS NOT NULL

  ) 
  WHERE seq = 1
  
)

, wbc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , wbc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE) AS delta_wbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND wbc IS NOT NULL


  ) 
  WHERE seq = 1

)

, hr AS(

  SELECT * FROM (
    SELECT
        pairs.stay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, hr.charttime, MINUTE) AS delta_heart_rate
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
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, rr.charttime, MINUTE) AS delta_resp_rate
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
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, mbp.charttime, MINUTE) AS delta_mbp
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
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, tmp.charttime, MINUTE) AS delta_temperature
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
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, glc.charttime, MINUTE) AS delta_glucose
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

, albumin AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , albumin
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_albumin
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND albumin IS NOT NULL


  ) 
  WHERE seq = 1

)

, aniongap AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , aniongap
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_aniongap
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND aniongap IS NOT NULL


  ) 
  WHERE seq = 1

)

, bicarbonate AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bicarbonate
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_bicarbonate
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND bicarbonate IS NOT NULL

  ) 
  WHERE seq = 1

)

, bun AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bun
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_bun
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND bun IS NOT NULL

  ) 
  WHERE seq = 1

)

, calcium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , calcium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_calcium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND calcium IS NOT NULL

  ) 
  WHERE seq = 1

)

, chloride AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , chloride
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_chloride
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND chloride IS NOT NULL

  ) 
  WHERE seq = 1

)

, creatinine AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , creatinine
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_creatinine
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND creatinine IS NOT NULL

  ) 
  WHERE seq = 1

)

, glucose_lab AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , glucose AS glucose_lab
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_glucose_lab
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND glucose IS NOT NULL

  ) 
  WHERE seq = 1

)

, sodium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , sodium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_sodium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND sodium IS NOT NULL

  ) 
  WHERE seq = 1

)

, potassium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , potassium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE) AS delta_potassium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND potassium IS NOT NULL

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , pairs.SaO2
  , pairs.SaO2_SpO2_delta_min AS delta_SpO2
  , pairs.SpO2
  , pairs.hidden_hypoxemia
  , hemoglobin.delta_hemoglobin
  , hemoglobin.hemoglobin
  , hematocrit.delta_hematocrit
  , hematocrit.hematocrit
  , mch.delta_mch
  , mch.mch
  , mchc.delta_mchc
  , mchc.mchc
  , mcv.delta_mcv
  , mcv.mcv
  , platelet.delta_platelet
  , platelet.platelet
  , rbc.delta_rbc
  , rbc.rbc
  , rdw.delta_rdw
  , rdw.rdw
  , wbc.delta_wbc
  , wbc.wbc
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
  , albumin.delta_albumin
  , albumin.albumin
  , aniongap.delta_aniongap
  , aniongap.aniongap
  , bicarbonate.delta_bicarbonate
  , bicarbonate.bicarbonate
  , bun.delta_bun
  , bun.bun
  , calcium.delta_calcium
  , calcium.calcium
  , chloride.delta_chloride
  , chloride.chloride
  , creatinine.delta_creatinine
  , creatinine.creatinine
  , glucose_lab.delta_glucose_lab
  , glucose_lab.glucose_lab
  , sodium.delta_sodium
  , sodium.sodium
  , potassium.delta_potassium
  , potassium.potassium

FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.subject_id = pairs.subject_id
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.subject_id = pairs.subject_id
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mch
ON mch.subject_id = pairs.subject_id
AND mch.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mchc
ON mchc.subject_id = pairs.subject_id
AND mchc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mcv
ON mcv.subject_id = pairs.subject_id
AND mcv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN platelet
ON platelet.subject_id = pairs.subject_id
AND platelet.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rbc
ON rbc.subject_id = pairs.subject_id
AND rbc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rdw
ON rdw.subject_id = pairs.subject_id
AND rdw.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN wbc
ON wbc.subject_id = pairs.subject_id
AND wbc.SaO2_timestamp = pairs.SaO2_timestamp

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

LEFT JOIN albumin
ON albumin.subject_id = pairs.subject_id
AND albumin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN aniongap
ON aniongap.subject_id = pairs.subject_id
AND aniongap.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bicarbonate
ON bicarbonate.subject_id = pairs.subject_id
AND bicarbonate.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bun
ON bun.subject_id = pairs.subject_id
AND bun.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN calcium
ON calcium.subject_id = pairs.subject_id
AND calcium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN chloride
ON chloride.subject_id = pairs.subject_id
AND chloride.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN creatinine
ON creatinine.subject_id = pairs.subject_id
AND creatinine.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glucose_lab
ON glucose_lab.subject_id = pairs.subject_id
AND glucose_lab.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sodium
ON sodium.subject_id = pairs.subject_id
AND sodium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN potassium
ON potassium.subject_id = pairs.subject_id
AND potassium.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp
