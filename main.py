import pandas as pd
from functools import reduce

def task1():
    # Read 2020 and 2021 VAERS Data csv and combine them
    a = pd.read_csv("2020VAERSData.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSData.csv", encoding="ISO-8859-1", engine='python')
    VAERSData = pd.concat([a, b], sort=False)

    # Read 2020 and 2021 VAERS Vax csv and combine them
    a = pd.read_csv("2020VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    VAERSVax = pd.concat([a, b], sort=False)
    # Remove all non Covid rows
    VAERSVax = VAERSVax[VAERSVax.VAX_TYPE.eq("COVID19")]

    # Read 2020 and 2021 VAERS Symptoms csv and combine them
    a = pd.read_csv("2020VAERSSYMPTOMS.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSSYMPTOMS.csv", encoding="ISO-8859-1", engine='python')
    VAERSSymptoms = pd.concat([a, b], sort=False)

    # Combine all 3 datasets
    CovidData = reduce(lambda x, y: pd.merge(x, y, on='VAERS_ID', how='outer', sort=False),
                       [VAERSData, VAERSVax, VAERSSymptoms])
    # Remove all non Covid rows again
    CovidData = CovidData[CovidData.VAX_TYPE.eq("COVID19")]

    # Save the file - End of Task 1 :)
    CovidData.to_csv('VAERS_COVID_DataAugust2021.csv', index=False)

    return CovidData


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Task1Set = task1()

# NOTES
# So we can have multiple rows of the same person, so same ID. Especially if they have more than 5 symptoms
# So we need to read the data, and then if they have an ID that we already came across (like on a hashmap) then we
# can just add the symptoms to the array of symptoms for that existing patient
# PLAN for task 2 ;(
# --> FIRST, trim data down to the columns for task2
#       VAERS_ID, AGE_YRS, SEX, VAX_NAME, RPT_Date, SYMPTOM, DIED, DATEDIED, SYMPTOM_TEXT
# --> SECOND, read the data line by line and store the stuff in a hashmap with VAERSID as the key
#       If the key exists already, we can add the symptoms to the array, if not, then we create a new entry
#       Structure of the data can be, ID is the key and some object for the value
#       The object has all the info + an array of symptoms which itself is an array [Symptom, SymptomVersion]
#       KEEP EMPTY SYMPTOM COLUMNS IN MIND - NEED TO ACCOUNT FOR THESE
# --> THIRD, convert this hasmap we have into a csv file somehow, lol good luck. Save it too
# --> FOURTH, create the 3 sotring algorithms to sort that hashmap with the ID :)) [Don't use CSV for it I think]