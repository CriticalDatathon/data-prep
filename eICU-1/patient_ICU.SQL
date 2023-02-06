DROP TABLE IF EXISTS `golden-rite-376204.eicu1_pulseOx.patient_ICU`;
CREATE TABLE `golden-rite-376204.eicu1_pulseOx.patient_ICU` AS

SELECT DISTINCT
    icu.patienthealthsystemstayid 
  , icu.patientunitstayid
  , CASE WHEN icu.gender = 0 THEN "F" 
         WHEN icu.gender = 1 THEN "M" 
    END AS gender
  , CASE WHEN icu.gender = 0 THEN 1 
         WHEN icu.gender = 1 THEN 0 
    END AS sex_female
  , CASE WHEN icu.age = "> 89" THEN "91" ELSE icu.age END AS anchor_age 
  , icu.ethnicity as race
  , CASE 
      WHEN (
        LOWER(icu.ethnicity) LIKE "%caucasian%" 
      ) THEN "White"
      WHEN (
        LOWER(icu.ethnicity) LIKE "%african american%"
      ) THEN "Black"
      WHEN (
         LOWER(icu.ethnicity) LIKE "%hispanic%"
      ) THEN "Hispanic"
      WHEN (
         LOWER(icu.ethnicity) LIKE "%asian%"
      ) THEN "Asian"
      ELSE "Other"
    END AS race_group

  , icu.admissionweight AS weight
  , icu.admissionheight AS height
  , CASE
      WHEN icu.admissionheight > 0
        THEN icu.admissionweight / (POWER(icu.admissionheight/100, 2)) 
      ELSE NULL
      END  
    AS BMI

  , icu.hospitaldischargeyear AS anchor_year_group
  , icu.unitvisitnumber AS icustay_seq
  , icu.icu_los_hours AS los_icu

  --, charlson.charlson_comorbidity_index AS CCI    
  --, sf.SOFA AS SOFA_admission

-- ICU stays
FROM `physionet-data.eicu_crd_derived.icustay_detail`
AS icu 

-- SOFA
--LEFT JOIN physionet-data.mimiciv_derived.first_day_sofa
--AS sofa
--ON icu.patientunitstayid = sofa.patientunitstayid 

-- Charlson 
--LEFT JOIN physionet-data.mimiciv_derived.charlson
--AS charlson
--ON icu.hadm_id = charlson.hadm_id

ORDER BY patienthealthsystemstayid, patientunitstayid