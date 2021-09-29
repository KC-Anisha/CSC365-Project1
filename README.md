# CSC 365 - Data Structures and Algorithms

## Project 1

### By: Anisha KC

### Project Description:
Consider the Vaccine Adverse Events Reporting System (VAERS) maintained by CDC (https://vaers.hhs.gov/) with datasets available since 1990. Attached you find the datasets for 2020 as well as the data for 2021 (reported until August 27, 2021 ). For those interested in further analyses, all datasets are available at https://vaers.hhs.gov/data/datasets.html

The database contains reports submitted regarding adverse effects following vaccination. For a sample of the data collected you can look at the VAERSForm_Mar2021.pdf



The datasets publicly available for analysis are de-identified, data being indexed by VAERS_ID, which is the code associated with the reports submitted regarding a given patient identified as having adverse event after a given vaccination.

Each dataset consists of three CSV files having the following information:



xxxxVAERSData.csv keeps track of the following information:

`VAERS_ID,RECVDATE,STATE,AGE_YRS,CAGE_YR,CAGE_MO,SEX,RPT_DATE,SYMPTOM_TEXT,DIED,DATEDIED,L_THREAT,ER_VISIT,HOSPITAL,HOSPDAYS,X_STAY,DISABLE,RECOVD,VAX_DATE,ONSET_DATE,NUMDAYS,LAB_DATA,V_ADMINBY,V_FUNDBY,OTHER_MEDS,CUR_ILL,HISTORY,PRIOR_VAX,SPLTTYPE,FORM_VERS,TODAYS_DATE,BIRTH_DEFECT,OFC_VISIT,ER_ED_VISIT,ALLERGIES`



xxxxVAERSSYMPTOMPS.csv keeps track of the following information:

`VAERS_ID,SYMPTOM1,SYMPTOMVERSION1,SYMPTOM2,SYMPTOMVERSION2,SYMPTOM3,SYMPTOMVERSION3,SYMPTOM4,SYMPTOMVERSION4,SYMPTOM5,SYMPTOMVERSION5
`

and

xxxxVAERSVAX.csv

`VAERS_ID,VAX_TYPE,VAX_MANU,VAX_LOT,VAX_DOSE_SERIES,VAX_ROUTE,VAX_SITE,VAX_NAME
`


### TASK 1

From the given datasets create one dataset containing only data regarding COVID-19 vaccination, where every row with a given VAERS_ID will contain all the data fields from VAERSData, followed by the VAERSVAX fields, followed by the VAERSSYMPTOMPS fields. The new dataset should be saved as as CSV file (VAERS_COVID_DataAugust2021.csv), with the Header containing all the variables names.

### TASK 2

Implement Quicksort, InsertionSort and another Sorting algorithm of your choice. Create a dataset from the set created in TASK 1, containing the following information: VAERS_ID, AGE_YRS, SEX, VAX_NAME, RPT_Date, SYMPTOM, DIED, DATEDIED, SYMPTOM_TEXT. Save the dataset as SYMPTOMDATA.csv. Since a VAERS report can contain up to 5 symptoms, the resulting dataset may contain several rows with the same VAERS_ID.  Utilize the sorts and sort your data by VAERS_ID. Use subsets of different sizes to analyze the time efficiency of the running time of the three algorithms. Include the results of your empirical analyses in the final report.

### TASK3

Utilizing the dataset you created for TASK2, sort the data, grouped by age group (<1 year, 1-3 years, 4-11, 12-18, 19-30, 31-40, 41-50, 51-60, 61-70, 71-80, > 80), then by gender, then by Vaccine Name, then by symptom. For each age group, compute the number of cases that have been reported as resulting in death. While there might be multiple symptoms reported, each VAERSID should be counted only once.
