-- Treatments: FiO2, RRT, Vasopressor, dose. Missing: ventilation, type.

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.treatment`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.treatment` AS

WITH
  bg AS (

  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , CASE 
        WHEN fio2 IS NOT NULL THEN fio2
        ELSE NULL
      END
      AS FiO2
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(bg.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_FiO2
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_bg` 
    AS bg
    ON bg.icustay_id = pairs.icustay_id
    AND bg.charttime <= pairs.SaO2_timestamp -- only looking at past values
    AND COALESCE(fio2) IS NOT NULL

  ) 
  WHERE seq = 1

)

, vp AS (

  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , norepinephrine_equivalent_dose
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(vp.starttime, pairs.SaO2_timestamp, MINUTE) AS delta_vp_start
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, vp.endtime, MINUTE)) ASC) AS seq -- pick the value closest to end

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.norepinephrine_equivalent_dose`
    AS vp
    ON vp.icustay_id = pairs.icustay_id
    AND (pairs.SaO2_timestamp BETWEEN vp.starttime AND vp.endtime)  
    AND vp.norepinephrine_equivalent_dose IS NOT NULL

  ) 
  WHERE seq = 1

) 

, rrt AS(

  SELECT * FROM(
    SELECT
      pairs.icustay_id
    , dialysis_present AS rrt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(rrt.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_rrt
    , ROW_NUMBER() OVER(PARTITION BY pairs.icustay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rrt.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_rrt` 
    AS rrt
    ON rrt.icustay_id = pairs.icustay_id
    AND rrt.charttime <= pairs.SaO2_timestamp -- just look at past values
    AND dialysis_present IS NOT NULL

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.icustay_id
  , pairs.SaO2_timestamp
  , vp.delta_vp_start
  , vp.norepinephrine_equivalent_dose
  , bg.delta_FiO2
  , bg.FiO2

  , rrt.delta_rrt
  , CASE
      WHEN rrt.rrt = 1 THEN 1
      ELSE 0
    END AS rrt

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN bg
ON bg.icustay_id = pairs.icustay_id
AND bg.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rrt
ON rrt.icustay_id = pairs.icustay_id
AND rrt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN vp
ON vp.icustay_id = pairs.icustay_id
AND vp.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, icustay_id, SaO2_timestamp ASC