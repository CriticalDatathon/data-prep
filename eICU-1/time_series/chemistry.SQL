-- Chemistry Values include albumin, aniongap, bicarbonate, bun, calcium, chloride, glucose (from the lab), sodium, potassium, ph

DROP TABLE IF EXISTS `db_name.eicu1_pulseOx.pairs_blood_vitals`;
CREATE TABLE `db_name.eicu1_pulseOx.pairs_blood_vitals` AS

WITH 

  albumin AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , albumin
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_albumin
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND albumin IS NOT NULL
  ) 
  WHERE seq = 1

)

, aniongap AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , aniongap
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_aniongap
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.eicu_crd_derived.pivoted_bg`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND aniongap IS NOT NULL


  ) 
  WHERE seq = 1

)

, bicarbonate AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bicarbonate
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_bicarbonate
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND bicarbonate IS NOT NULL

  ) 
  WHERE seq = 1

)

, bun AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bun
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_bun
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND bun IS NOT NULL

  ) 
  WHERE seq = 1

)

, calcium AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , calcium
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_calcium
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND calcium IS NOT NULL

  ) 
  WHERE seq = 1

)

, chloride AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , chloride
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_chloride
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND chloride IS NOT NULL

  ) 
  WHERE seq = 1

)

, creatinine AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , creatinine
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_creatinine
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND creatinine IS NOT NULL

  ) 
  WHERE seq = 1

)

, glucose_lab AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , glucose AS glucose_lab
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_glucose_lab
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND glucose IS NOT NULL

  ) 
  WHERE seq = 1

)

, sodium AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , sodium
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_sodium
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND sodium IS NOT NULL

  ) 
  WHERE seq = 1

)

, potassium AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , potassium
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_potassium
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND potassium IS NOT NULL

  ) 
  WHERE seq = 1

)

, ph AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ph
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ph
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.eicu_crd_derived.pivoted_bg` 
    AS bg
    ON bg.patientunitstayid = pairs.patientunitstayid
    AND ph IS NOT NULL

  ) 
  WHERE seq = 1

)

, lactate AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , lactate
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_lactate
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS chem
    ON chem.patientunitstayid = pairs.patientunitstayid
    AND lactate IS NOT NULL

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.patientunitstayid
  , pairs.SaO2_offset
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
  , ph.delta_ph
  , ph.ph
  , lactate.delta_lactate
  , lactate.lactate

FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN albumin
ON albumin.patientunitstayid = pairs.patientunitstayid
AND albumin.SaO2_offset = pairs.SaO2_offset

LEFT JOIN aniongap
ON aniongap.patientunitstayid = pairs.patientunitstayid
AND aniongap.SaO2_offset = pairs.SaO2_offset

LEFT JOIN bicarbonate
ON bicarbonate.patientunitstayid = pairs.patientunitstayid
AND bicarbonate.SaO2_offset = pairs.SaO2_offset

LEFT JOIN bun
ON bun.patientunitstayid = pairs.patientunitstayid
AND bun.SaO2_offset = pairs.SaO2_offset

LEFT JOIN calcium
ON calcium.patientunitstayid = pairs.patientunitstayid
AND calcium.SaO2_offset = pairs.SaO2_offset

LEFT JOIN chloride
ON chloride.patientunitstayid = pairs.patientunitstayid
AND chloride.SaO2_offset = pairs.SaO2_offset

LEFT JOIN creatinine
ON creatinine.patientunitstayid = pairs.patientunitstayid
AND creatinine.SaO2_offset = pairs.SaO2_offset

LEFT JOIN glucose_lab
ON glucose_lab.patientunitstayid = pairs.patientunitstayid
AND glucose_lab.SaO2_offset = pairs.SaO2_offset

LEFT JOIN sodium
ON sodium.patientunitstayid = pairs.patientunitstayid
AND sodium.SaO2_offset = pairs.SaO2_offset

LEFT JOIN potassium
ON potassium.patientunitstayid = pairs.patientunitstayid
AND potassium.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ph
ON ph.patientunitstayid = pairs.patientunitstayid
AND ph.SaO2_offset = pairs.SaO2_offset

LEFT JOIN lactate
ON lactate.patientunitstayid = pairs.patientunitstayid
AND lactate.SaO2_offset = pairs.SaO2_offset

ORDER BY patientunitstayid, SaO2_offset
