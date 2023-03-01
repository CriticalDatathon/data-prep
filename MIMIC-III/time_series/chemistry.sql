-- Chemistry Values include bicarbonate, calcium, chloride, glucose (from the lab), sodium, potassium, pH, bun, albumin, aniongap, lactate and creatinine.

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.pairs_blood_vitals`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.pairs_blood_vitals` AS

WITH
potassium AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, potassium.charttime, MINUTE) AS delta_potassium
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, potassium.charttime, MINUTE)) ASC ) AS seq
      , potassium.potassium

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` potassium

    ON potassium.icustay_id = pairs.icustay_id
    AND potassium.potassium IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, bicarbonate AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, bicarbonate.charttime, MINUTE) AS delta_bicarbonate
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bicarbonate.charttime, MINUTE)) ASC ) AS seq
      , bicarbonate.bicarbonate

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` bicarbonate

    ON bicarbonate.icustay_id = pairs.icustay_id
    AND bicarbonate.bicarbonate IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, bun AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, bun.charttime, MINUTE) AS delta_bun
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bun.charttime, MINUTE)) ASC ) AS seq
      , bun.bun

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` bun

    ON bun.subject_id = pairs.subject_id
    AND bun.bun IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, albumin AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, albumin.charttime, MINUTE) AS delta_albumin
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, albumin.charttime, MINUTE)) ASC ) AS seq
      , albumin.albumin

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` albumin

    ON albumin.subject_id = pairs.subject_id
    AND albumin.albumin IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, aniongap AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, aniongap.charttime, MINUTE) AS delta_aniongap
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, aniongap.charttime, MINUTE)) ASC ) AS seq
      , aniongap.aniongap

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` aniongap

    ON aniongap.subject_id = pairs.subject_id
    AND aniongap.aniongap IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, lactate AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, lactate.charttime, MINUTE) AS delta_lactate
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, lactate.charttime, MINUTE)) ASC ) AS seq
      , lactate.lactate

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` lactate

    ON lactate.subject_id = pairs.subject_id
    AND lactate.lactate IS NOT NULL
      
    ) 
    WHERE seq = 1
)
, ph AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, ph.charttime, MINUTE) AS delta_ph
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, ph.charttime, MINUTE)) ASC ) AS seq
      , ph.ph

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` ph

    ON ph.icustay_id = pairs.icustay_id
    AND ph.ph IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, calcium AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, calcium.charttime, MINUTE) AS delta_calcium
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, calcium.charttime, MINUTE)) ASC ) AS seq
      , calcium.calcium

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` calcium

    ON calcium.icustay_id = pairs.icustay_id
    AND calcium.calcium IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, chloride AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, chloride.charttime, MINUTE) AS delta_chloride
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chloride.charttime, MINUTE)) ASC ) AS seq
      , chloride.chloride

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` chloride

    ON chloride.icustay_id = pairs.icustay_id
    AND chloride.chloride IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, glucose_lab AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, glucose_lab.charttime, MINUTE) AS delta_glucose_lab
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, glucose_lab.charttime, MINUTE)) ASC ) AS seq
      , glucose_lab.glucose as glucose_lab

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` glucose_lab

    ON glucose_lab.icustay_id = pairs.icustay_id
    AND glucose_lab.glucose IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, sodium AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, sodium.charttime, MINUTE) AS delta_sodium
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, sodium.charttime, MINUTE)) ASC ) AS seq
      , sodium.sodium

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` sodium

    ON sodium.icustay_id = pairs.icustay_id
    AND sodium.sodium IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, creatinine AS(

  SELECT * FROM (
    SELECT
        pairs.icustay_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, creatinine.charttime, MINUTE) AS delta_creatinine
      , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, creatinine.charttime, MINUTE)) ASC ) AS seq
      , creatinine.creat as creatinine

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.kdigo_creatinine` creatinine

    ON creatinine.icustay_id = pairs.icustay_id
    AND creatinine.creat IS NOT NULL
      
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
  , bicarbonate.delta_bicarbonate
  , bicarbonate.bicarbonate
  , ph.delta_ph
  , ph.ph
  , bun.delta_bun
  , bun.bun
  , albumin.delta_albumin
  , albumin.albumin
  , aniongap.delta_aniongap
  , aniongap.aniongap
  , lactate.delta_lactate
  , lactate.lactate
  , calcium.delta_calcium
  , calcium.calcium
  , chloride.delta_chloride
  , chloride.chloride
  , glucose_lab.delta_glucose_lab
  , glucose_lab.glucose_lab
  , sodium.delta_sodium
  , sodium.sodium
  , potassium.delta_potassium
  , potassium.potassium
  , creatinine.delta_creatinine
  , creatinine.creatinine

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN bicarbonate
ON bicarbonate.icustay_id = pairs.icustay_id
AND bicarbonate.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ph
ON ph.icustay_id = pairs.icustay_id
AND ph.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN calcium
ON calcium.icustay_id = pairs.icustay_id
AND calcium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bun
ON bun.subject_id = pairs.subject_id
AND bun.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN albumin
ON albumin.subject_id = pairs.subject_id
AND albumin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN aniongap
ON aniongap.subject_id = pairs.subject_id
AND aniongap.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN lactate
ON lactate.subject_id = pairs.subject_id
AND lactate.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN chloride
ON chloride.icustay_id = pairs.icustay_id
AND chloride.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glucose_lab
ON glucose_lab.icustay_id = pairs.icustay_id
AND glucose_lab.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sodium
ON sodium.icustay_id = pairs.icustay_id
AND sodium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN creatinine
ON creatinine.icustay_id = pairs.icustay_id
AND creatinine.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN potassium
ON potassium.icustay_id = pairs.icustay_id
AND potassium.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, icustay_id, SaO2_timestamp