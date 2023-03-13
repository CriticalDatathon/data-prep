-- Coagualtion Values include d_dimer, fibrinogen, thrombin, inr (international normalized ratio), pt (prothrombin time), ptt (partial thromboplastin time)

DROP TABLE IF EXISTS `golden-rite-376204.eicu1_pulseOx.coagulation`;
CREATE TABLE `golden-rite-376204.eicu1_pulseOx.coagulation` AS

WITH 


 fibrinogen AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , fibrinogen
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_fibrinogen
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.eicu1_pulseOx.pivoted_lab2` 
    AS coag
    ON coag.patientunitstayid = pairs.patientunitstayid
    AND fibrinogen IS NOT NULL


  ) 
  WHERE seq = 1

)

, inr AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , inr
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_inr
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.eicu1_pulseOx.pivoted_lab2` 
    AS coag
    ON coag.patientunitstayid = pairs.patientunitstayid
    AND inr IS NOT NULL

  ) 
  WHERE seq = 1

)

, pt AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , pt
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_pt
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.eicu1_pulseOx.pivoted_lab2` 
    AS coag
    ON coag.patientunitstayid = pairs.patientunitstayid
    AND pt IS NOT NULL

  ) 
  WHERE seq = 1

)

, ptt AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ptt
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ptt
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.eicu1_pulseOx.pivoted_lab2` 
    AS coag
    ON coag.patientunitstayid = pairs.patientunitstayid
    AND ptt IS NOT NULL

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.patientunitstayid
  , pairs.SaO2_offset
  , fibrinogen.delta_fibrinogen
  , fibrinogen.fibrinogen
  , inr.delta_inr
  , inr.inr
  , pt.delta_pt
  , pt.pt
  , ptt.delta_ptt
  , ptt.ptt

FROM `golden-rite-376204.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN fibrinogen
ON fibrinogen.patientunitstayid = pairs.patientunitstayid
AND fibrinogen.SaO2_offset = pairs.SaO2_offset

LEFT JOIN inr
ON inr.patientunitstayid = pairs.patientunitstayid
AND inr.SaO2_offset = pairs.SaO2_offset

LEFT JOIN pt
ON pt.patientunitstayid = pairs.patientunitstayid
AND pt.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ptt
ON ptt.patientunitstayid = pairs.patientunitstayid
AND ptt.SaO2_offset = pairs.SaO2_offset

ORDER BY patientunitstayid, SaO2_offset
