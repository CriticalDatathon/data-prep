-- Blood Values include hemoglobin, hematocrit, platelet, wbc, mch, mchc, mcv, rbc, rdw

DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.blood_count`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.blood_count` AS

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

, platelet AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, platelet.charttime, MINUTE) AS delta_platelet
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, platelet.charttime, MINUTE)) ASC ) AS seq
      , platelet.platelet

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` platelet

    ON platelet.subject_id = pairs.subject_id
    AND platelet.platelet IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, wbc AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, wbc.charttime, MINUTE) AS delta_wbc
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, wbc.charttime, MINUTE)) ASC ) AS seq
      , wbc.wbc

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciii_derived.pivoted_lab` wbc

    ON wbc.subject_id = pairs.subject_id
    AND wbc.wbc IS NOT NULL
      
    ) 
    WHERE seq = 1
)


, mch AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, mch.charttime, MINUTE) AS delta_mch
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, mch.charttime, MINUTE)) ASC ) AS seq
      , mch.mch

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.pivoted_lab_extended` mch

    ON mch.subject_id = pairs.subject_id
    AND mch.mch IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, mchc AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, mchc.charttime, MINUTE) AS delta_mchc
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, mchc.charttime, MINUTE)) ASC ) AS seq
      , mchc.mchc

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.pivoted_lab_extended` mchc

    ON mchc.subject_id = pairs.subject_id
    AND mchc.mchc IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, mcv AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, mcv.charttime, MINUTE) AS delta_mcv
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, mcv.charttime, MINUTE)) ASC ) AS seq
      , mcv.mcv

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.pivoted_lab_extended` mcv

    ON mcv.subject_id = pairs.subject_id
    AND mcv.mcv IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, rbc AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, rbc.charttime, MINUTE) AS delta_rbc
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rbc.charttime, MINUTE)) ASC ) AS seq
      , rbc.rbc

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.pivoted_lab_extended` rbc

    ON rbc.subject_id = pairs.subject_id
    AND rbc.rbc IS NOT NULL
      
    ) 
    WHERE seq = 1
)

, rdw AS(

  SELECT * FROM (
    SELECT
        pairs.subject_id
      , pairs.SaO2_timestamp
      , TIMESTAMP_DIFF(pairs.SaO2_timestamp, rdw.charttime, MINUTE) AS delta_rdw
      , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                          ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, rdw.charttime, MINUTE)) ASC ) AS seq
      , rdw.rdw

    FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `golden-rite-376204.mimiciii_pulseOx.pivoted_lab_extended` rdw

    ON rdw.subject_id = pairs.subject_id
    AND rdw.rdw IS NOT NULL
      
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
  , platelet.delta_platelet
  , platelet.platelet
  , wbc.delta_wbc
  , wbc.wbc
  , mch.delta_mch
  , mch.mch
  , mchc.delta_mchc
  , mchc.mchc
  , mcv.delta_mcv
  , mcv.mcv
  , rbc.delta_rbc
  , rbc.rbc
  , rdw.delta_rdw
  , rdw.rdw

FROM `golden-rite-376204.mimiciii_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.icustay_id = pairs.icustay_id
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.icustay_id = pairs.icustay_id
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN platelet
ON platelet.subject_id = pairs.subject_id
AND platelet.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN wbc
ON wbc.subject_id = pairs.subject_id
AND wbc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mch
ON mch.subject_id = pairs.subject_id
AND mch.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mchc
ON mchc.subject_id = pairs.subject_id
AND mchc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mcv
ON mcv.subject_id = pairs.subject_id
AND mcv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rbc
ON rbc.subject_id = pairs.subject_id
AND rbc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rdw
ON rdw.subject_id = pairs.subject_id
AND rdw.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, icustay_id, SaO2_timestamp