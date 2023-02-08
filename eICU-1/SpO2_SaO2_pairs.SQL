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
  AND sao2_table.labresultoffset IS NOT NULL

  GROUP BY patientunitstayid, SaO2_offset
  ORDER BY SaO2_offset ASC
)

, art_line AS (

  -- vitalAperiodic Table -> cardiacoutput needs an arterial line
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd.vitalaperiodic`
  WHERE cardiacoutput IS NOT NULL AND cardiacoutput >0.8
  GROUP BY patientunitstayid

  UNION DISTINCT
  -- vitalPeriodic Table -> systemicmean AND pamean need an arterial line
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd.vitalperiodic`
  WHERE (systemicmean IS NOT NULL AND systemicmean > 0)
  OR (pamean IS NOT NULL AND pamean > 0)
  GROUP BY patientunitstayid

  UNION DISTINCT
  -- nurseCharting Table -> everyone with invasive blood pressure
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd.nursecharting`
  WHERE LOWER(nursingchartcelltypevallabel) LIKE "%arter%"
  GROUP BY patientunitstayid

  UNION DISTINCT
  -- medication Table -> everyone mentioning arterial
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd.medication`
  WHERE LOWER(routeadmin) like "%arterial%"
  GROUP BY patientunitstayid

  UNION DISTINCT
  -- pivoted vital Table -> everyone with 
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd_derived.pivoted_vital`
  WHERE ibp_systolic IS NOT NULL
  OR ibp_diastolic IS NOT NULL
  OR ibp_mean IS NOT NULL
  GROUP BY patientunitstayid

  UNION DISTINCT 
  -- pitvoted 
  SELECT patientunitstayid, 1 AS arterial_line
  FROM `physionet-data.eicu_crd_derived.pivoted_infusion`
  where dopamine = 1
  OR dobutamine = 1
  OR norepinephrine = 1
  OR phenylephrine = 1
  OR epinephrine = 1
  OR vasopressin = 1
  OR milrinone = 1
  GROUP BY patientunitstayid
)

SELECT DISTINCT
    pairs.patientunitstayid
  , SaO2_offset
  , sao2_vals.labResult AS SaO2
  , (SpO2_offset - SaO2_offset) AS delta_SpO2
  , spo2_vals.spo2 AS SpO2
  , CASE WHEN spo2_vals.spo2 >= 88 AND sao2_vals.labResult < 88 THEN 1 ELSE 0 END AS hidden_hypoxemia
  , CASE WHEN arterial_line = 1 THEN 1 ELSE 0 END AS arterial_line

FROM pairs

LEFT JOIN art_line
ON art_line.patientunitstayid = pairs.patientunitstayid

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
AND sao2_vals.labResult >= 80  -- cut-off point to make sure we're looking only at arterial values
--AND arterial_line = 1 -- make sure we have an arterial line

ORDER BY patientunitstayid, SaO2_offset ASC
