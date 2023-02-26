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


, potassium AS(

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
  , rr.delta_resprate
  , rr.resprate
  , tmp.delta_temperature
  , tmp.temperature
  , glc.delta_glucose
  , glc.glucose
  , bicarbonate.delta_bicarbonate
  , bicarbonate.bicarbonate
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

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.icustay_id = pairs.icustay_id
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.icustay_id = pairs.icustay_id
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rr
ON rr.icustay_id = pairs.icustay_id
AND rr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN tmp
ON tmp.icustay_id = pairs.icustay_id
AND tmp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glc
ON glc.icustay_id = pairs.icustay_id
AND glc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bicarbonate
ON bicarbonate.icustay_id = pairs.icustay_id
AND bicarbonate.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN calcium
ON calcium.icustay_id = pairs.icustay_id
AND calcium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN chloride
ON chloride.icustay_id = pairs.icustay_id
AND chloride.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glucose_lab
ON glucose_lab.icustay_id = pairs.icustay_id
AND glucose_lab.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sodium
ON sodium.icustay_id = pairs.icustay_id
AND sodium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN potassium
ON potassium.icustay_id = pairs.icustay_id
AND potassium.SaO2_timestamp = pairs.SaO2_timestamp