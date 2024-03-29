-- enzyme Values include liver/abdominal markers from the lab
-- alt, alp, ast, bilirubin_total, bilirubin_direct, ck_cpk, ck_mb, ld_ldh

-- bilirubin_indirect does not exist in eICU
-- -- ggt missing

DROP TABLE IF EXISTS `db_name.eicu1_pulseOx.enzyme`;
CREATE TABLE `db_name.eicu1_pulseOx.enzyme` AS

WITH 

  alt AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , alt
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_alt
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND alt IS NOT NULL
  ) 
  WHERE seq = 1

)

, alp AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , alp
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_alp
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND alp IS NOT NULL
  ) 
  WHERE seq = 1

)

, ast AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ast
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ast
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ast IS NOT NULL

  ) 
  WHERE seq = 1

)

, bilirubin_total AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bilirubin_total
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_bilirubin_total
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND bilirubin_total IS NOT NULL

  ) 
  WHERE seq = 1

)

, bilirubin_direct AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bilirubin_direct
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_bilirubin_direct
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND bilirubin_direct IS NOT NULL

  ) 
  WHERE seq = 1

)

-- bilirubin_indirect missing

, ck_cpk AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ck_cpk
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ck_cpk
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ck_cpk IS NOT NULL

  ) 
  WHERE seq = 1

)

, ck_mb AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ck_mb AS ck_mb
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ck_mb
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ck_mb IS NOT NULL

  ) 
  WHERE seq = 1

)

-- ggt missing

, ld_ldh AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ld_ldh
    , pairs.SaO2_offset
    , (chartoffset - SaO2_offset) AS delta_ld_ldh
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_offset
                        ORDER BY ABS(chartoffset - SaO2_offset) ASC) AS seq

    FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `db_name.eicu1_pulseOx.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ld_ldh IS NOT NULL

  ) 
  WHERE seq = 1

)


SELECT 
    pairs.patientunitstayid
  , pairs.SaO2_offset
  , alt.delta_alt
  , alt.alt
  , alp.delta_alp
  , alp.alp
  , ast.delta_ast
  , ast.ast
  , bilirubin_total.delta_bilirubin_total
  , bilirubin_total.bilirubin_total
  , bilirubin_direct.delta_bilirubin_direct
  , bilirubin_direct.bilirubin_direct
  , ck_cpk.delta_ck_cpk
  , ck_cpk.ck_cpk
  , ck_mb.delta_ck_mb
  , ck_mb.ck_mb
  , ld_ldh.delta_ld_ldh
  , ld_ldh.ld_ldh

FROM `db_name.eicu1_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN alt
ON alt.patientunitstayid = pairs.patientunitstayid
AND alt.SaO2_offset = pairs.SaO2_offset

LEFT JOIN alp
ON alp.patientunitstayid = pairs.patientunitstayid
AND alp.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ast
ON ast.patientunitstayid = pairs.patientunitstayid
AND ast.SaO2_offset = pairs.SaO2_offset

LEFT JOIN bilirubin_total
ON bilirubin_total.patientunitstayid = pairs.patientunitstayid
AND bilirubin_total.SaO2_offset = pairs.SaO2_offset

LEFT JOIN bilirubin_direct
ON bilirubin_direct.patientunitstayid = pairs.patientunitstayid
AND bilirubin_direct.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ck_cpk
ON ck_cpk.patientunitstayid = pairs.patientunitstayid
AND ck_cpk.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ck_mb
ON ck_mb.patientunitstayid = pairs.patientunitstayid
AND ck_mb.SaO2_offset = pairs.SaO2_offset

LEFT JOIN ld_ldh
ON ld_ldh.patientunitstayid = pairs.patientunitstayid
AND ld_ldh.SaO2_offset = pairs.SaO2_offset

ORDER BY patientunitstayid, SaO2_offset
