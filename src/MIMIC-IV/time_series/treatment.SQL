-- Treatments: ventilation, type, FiO2, RRT, Vasopressor, AND dose

DROP TABLE IF EXISTS `golden-rite-376204.mimiciv_pulseOx.treatment`;
CREATE TABLE `golden-rite-376204.mimiciv_pulseOx.treatment` AS

WITH 
  vent AS (

  SELECT * FROM(
    SELECT
      pairs.stay_id
    , ventilation_status
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(vent.starttime, pairs.SaO2_timestamp, MINUTE) AS delta_vent_start
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, vent.endtime, MINUTE)) ASC) AS seq -- pick the value closest to end

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.ventilation` 
    AS vent
    ON vent.stay_id = pairs.stay_id
    AND (pairs.SaO2_timestamp BETWEEN vent.starttime AND vent.endtime)  
    AND vent.ventilation_status IS NOT NULL

  ) 
  WHERE seq = 1

) 

,  bg AS (

  SELECT * FROM(
    SELECT
      pairs.subject_id
    , CASE 
        WHEN fio2 IS NOT NULL THEN fio2
        WHEN fio2_chartevents IS NOT NULL THEN fio2_chartevents 
        ELSE NULL
      END
      AS FiO2
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(bg.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_FiO2
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.bg` 
    AS bg
    ON bg.subject_id = pairs.subject_id
    AND bg.charttime <= pairs.SaO2_timestamp -- only looking at past values
    AND COALESCE(fio2,fio2_chartevents) IS NOT NULL

  ) 
  WHERE seq = 1

)

, rrt AS(

  SELECT * FROM(
    SELECT
      pairs.stay_id
    , dialysis_present AS rrt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(rrt.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_rrt
    , ROW_NUMBER() OVER(PARTITION BY pairs.stay_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rrt.charttime, MINUTE)) ASC) AS seq

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.rrt` 
    AS rrt
    ON rrt.stay_id = pairs.stay_id
    AND rrt.charttime <= pairs.SaO2_timestamp -- just look at past values
    AND dialysis_present IS NOT NULL

  ) 
  WHERE seq = 1

)

, vp AS (

  SELECT * FROM(
    SELECT
      pairs.stay_id
    , norepinephrine_equivalent_dose
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(vp.starttime, pairs.SaO2_timestamp, MINUTE) AS delta_vp_start
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, vp.endtime, MINUTE)) ASC) AS seq -- pick the value closest to end

    FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.norepinephrine_equivalent_dose` 
    AS vp
    ON vp.stay_id = pairs.stay_id
    AND (pairs.SaO2_timestamp BETWEEN vp.starttime AND vp.endtime)  
    AND vp.norepinephrine_equivalent_dose IS NOT NULL

  ) 
  WHERE seq = 1

) 

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , vent.delta_vent_start
  , vent.ventilation_status

  , CASE
      WHEN vent.ventilation_status IN("Tracheostomy", "InvasiveVent")
      THEN 1
      ELSE 0
    END AS invasive_vent

  , CASE
      WHEN vent.ventilation_status IS NOT NULL THEN bg.delta_FiO2
      ELSE NULL
    END AS delta_FiO2

  , CASE
      WHEN vent.ventilation_status IS NOT NULL THEN bg.FiO2
      ELSE NULL
    END AS FiO2

  , rrt.delta_rrt
  , CASE
      WHEN rrt.rrt = 1 THEN 1
      ELSE 0
    END AS rrt

  , vp.delta_vp_start
  , vp.norepinephrine_equivalent_dose

FROM `golden-rite-376204.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN vent
ON vent.stay_id = pairs.stay_id
AND pairs.SaO2_timestamp = vent.SaO2_timestamp

LEFT JOIN bg
ON bg.subject_id = pairs.subject_id
AND bg.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rrt
ON rrt.stay_id = pairs.stay_id
AND rrt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN vp
ON vp.stay_id = pairs.stay_id
AND vp.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp ASC
