-- Limiting variable: SaO2
-- We fetch all possible SaO2. Each of these is aligned with the closest SpO2 value, in a 90min window to the past

DROP TABLE IF EXISTS `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs`;
CREATE TABLE `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` AS

-- Auxiliary to fetch the SaO2 timestamps and get the corresponding SpO2
WITH pairs AS (
  SELECT
    sao2_table.patientunitstayid
  , sao2_table.labresultoffset AS SaO2_offset
  , MAX(spo2_table.chartoffset) AS SpO2_offset  

  FROM `physionet-data.eicu_crd.lab` AS sao2_table

  LEFT JOIN(
    SELECT 
      patientunitstayid
    , spo2
    , chartoffset
    FROM `physionet-data.eicu_crd_derived.pivoted_vital` spo2_table
  )
  AS spo2_table
  ON spo2_table.patientunitstayid = sao2_table.patientunitstayid

  -- each ABG-measured sao2_table is matched with the closest SpO2 value recorded within the previous 90 minutes"
  AND spo2_table.chartoffset > 0 -- positive values that refer to this stay
  AND (sao2_table.labresultoffset - spo2_table.chartoffset) <= 90
  AND (sao2_table.labresultoffset - spo2_table.chartoffset) >= 0

  -- Only for SaO2 values
  WHERE sao2_table.labname = "O2 Sat (%)"  -- SaO2
  AND sao2_table.labresultoffset > 0 -- positive values that refer to this stay
  AND spo2_table.chartoffset IS NOT NULL
  GROUP BY patientunitstayid, SaO2_offset
  ORDER BY SaO2_offset ASC
)

SELECT DISTINCT
    pairs.patientunitstayid
  , SaO2_offset
  , sao2_vals.labresulttext AS SaO2
  , (SpO2_offset - SaO2_offset) AS delta_SpO2
  , spo2_vals.spo2 AS SpO2
  --, CASE WHEN spo2_vals.spo2 >= 88 AND sao2_vals.valuenum < 88 THEN 1 ELSE 0 END AS hidden_hypoxemia

FROM pairs

LEFT JOIN `physionet-data.eicu_crd.lab`
AS sao2_vals
ON sao2_vals.patientunitstayid = pairs.patientunitstayid
AND sao2_vals.labresultoffset = pairs.SaO2_offset
AND sao2_vals.labname = "O2 Sat (%)"  -- SaO2


LEFT JOIN `physionet-data.eicu_crd_derived.pivoted_vital`
AS spo2_vals
ON spo2_vals.patientunitstayid = pairs.patientunitstayid
AND spo2_vals.chartoffset = pairs.SpO2_offset

WHERE spo2_vals.spo2 IS NOT NULL

ORDER BY patientunitstayid, SaO2_offset ASC
