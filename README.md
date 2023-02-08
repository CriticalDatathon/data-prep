# Pulse Oximetry Correction Models - Data Extraction

## How to Get Started
Once you're credentialed on PhysioNet:

[Recommended Environment: BigQuery](https://console.cloud.google.com/bigquery)

[Shared Project on BigQuery](https://console.cloud.google.com/bigquery?project=golden-rite-376204)

[Variables to collect](https://docs.google.com/spreadsheets/d/16w0sDQiFTde9O5jp4JLtl91oONw9g16B-oj9Eo9Rr0E/edit#gid=0)

[Drive with Papers / Documents](https://drive.google.com/drive/u/0/folders/1K0xnxcyo4t3rqzAcx72rJceLjVg28xkh)

[MIMIC Derived Tables](https://github.com/MIT-LCP/mimic-code)

[MIMIC Documentation](https://mimic.mit.edu)

[eICU Derived Tables](https://github.com/MIT-LCP/eicu-code/)

[eICU Documentation](https://eicu-crd.mit.edu/)

[PhysioNet](https://physionet.org/)

## Variables to Extract (incomplete, refer to Google Sheets for now)

### Fixed per Patient / ICU Stay
* Age
* Sex
* Race / Ethnicity
* Body Mass Index (BMI)
* Hospital / ICU id
* Charlson Comorbidity Index (CCI)
* Admission Sequential Organ Failure Assessment (SOFA) Score

### Time Series
#### Oxygen Saturation
* SpO2
* SaO2

#### Vital Signs
* Heart Rate
* Arterial Blood Pressure systolic
* Arterial Blood Pressure diastolic
* Arterial Blood Pressure mean
* Respiratory Rate
* Temperature
* Glucose

### Lab Values
* Hemoglobin
* Platelets
* WBC
* Bilirubin
* Lactate
* pH
* Creatinine
* Bicarbonate
* Sodium
* Potassium
* Chloride
* Blood Urea Nitrogen (BUN) 

#### ICU Care
* Ventilation
* RRT
* Vasopressors

