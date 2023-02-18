DROP TABLE IF EXISTS `golden-rite-376204.mimiciii_pulseOx.patient_ICU`;
CREATE TABLE `golden-rite-376204.mimiciii_pulseOx.patient_ICU` AS

SELECT DISTINCT
icu.subject_id
, icu.icustay_id
, icu.hadm_id
, icu.gender
, CASE WHEN icu.gender = "F" THEN 1 ELSE 0 END AS sex_female
, icu.admission_age
, icu.first_hosp_stay
, icu.first_icu_stay
, icu.icustay_seq
, icu.admittime
, icu.dischtime
, icu.intime as icu_intime
, icu.outtime as icu_outtime
, icu.los_hospital
, icu.los_icu
, icu.ethnicity
, icu.ethnicity_grouped
, icu.icustay_seq
, sf.sofa
, ad.language
, heightweight.weight_first as weight
, heightweight.height_first as height
, heightweight.weight_first / (POWER(heightweight.height_first/100, 2)) AS BMI

-- ICU stays
FROM physionet-data.mimiciii_derived.icustay_detail
AS icu

-- Hospital Admissions
LEFT JOIN physionet-data.mimiciii_clinical.admissions
AS ad
ON ad.hadm_id=icu.hadm_id

-- Height and Weight
LEFT JOIN physionet-data.mimiciii_derived.heightweight
AS heightweight
ON heightweight.icustay_id=icu.icustay_id

-- SOFA
LEFT JOIN physionet-data.mimiciii_derived.sofa
AS sf
ON sf.icustay_id=icu.icustay_id

