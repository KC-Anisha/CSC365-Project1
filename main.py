import json

import pandas as pd
from functools import reduce


class CovidObject:
    def __init__(self, age, sex, vaxName, rptDate, symptoms, died, dateDied, symptomText):
        self.age = age
        self.sex = sex
        self.vaxName = vaxName
        self.rptDate = rptDate
        self.symptoms = symptoms
        self.died = died
        self.dateDied = dateDied
        self.symptomText = symptomText

    # def to_dict(self) -> dict:
    #     return {
    #         "AGE_YRS": self.age,
    #         "SEX": self.sex,
    #         "VAX_NAME": self.vaxName,
    #         "RPT_DATE": self.rptDate,
    #         "SYMPTOMS": self.symptoms.to_dict(),
    #         "DIED": self.died,
    #         "DATEDIED": self.dateDied,
    #         "SYMPTOM_TEXT": self.symptomText,
    #     }


class SymptomObject:
    def __init__(self, symptomName, symptomVersion):
        self.symptomName = symptomName
        self.symptomVersion = symptomVersion

    def __repr__(self):
        return (self.symptomName + "; " + str(self.symptomVersion))

    # def to_dict(self) -> dict:
    #     return {
    #         "SYMPTOM": self.symptomName,
    #         "SYMPTOMVERSION": self.symptomVersion,
    #     }


def combineDataSets():
    # Read 2020 and 2021 VAERS Data csv and combine them
    a = pd.read_csv("2020VAERSData.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSData.csv", encoding="ISO-8859-1", engine='python')
    VAERSData = pd.concat([a, b], sort=False).drop_duplicates().reset_index(drop=True)

    # Read 2020 and 2021 VAERS Vax csv and combine them
    a = pd.read_csv("2020VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    VAERSVax = pd.concat([a, b], sort=False).drop_duplicates(subset='VAERS_ID').reset_index(drop=True)
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

    # Return the data
    return CovidData


if __name__ == '__main__':
    # Combine all the datasets (all CSV file) and get all the covid data from them
    combinedDataSet = combineDataSets()

    # Convert the data into a JSON format for easier looping
    covidJsonData = json.loads(pd.DataFrame.to_json(combinedDataSet, orient='records'))

    # Dictionary(HashMap) to store data
    hashMap = {}

    # Let's keep track of the highest number of symptoms [113]
    highestNumOfSymptoms = 0
    idOfMostSymptoms = 0

    # Loop through the data and store it in a hashmap
    for row in covidJsonData:
        vaersId = row["VAERS_ID"]
        # If ID is already in the HashMap, just update the symptoms for it
        if vaersId in hashMap.keys():
            # Update the existing object with the additional symptoms
            obj = hashMap[vaersId]
            finalSymptoms = obj["Symptoms"]
            # Go through all symptoms and create a new symptoms array
            newSymptoms = []
            for x in range(0, 5):
                if row["SYMPTOM%d"%(x+1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (row["SYMPTOM%d"%(x+1)], row["SYMPTOMVERSION%d"%(x+1)])))
            # Append the arrays and update the hashmap
            finalSymptoms.extend(newSymptoms)
            obj["Symptoms"] = finalSymptoms
            hashMap[vaersId] = obj
        # If ID is not in Hashmap, create a new entry
        else:
            # Create an array of symptoms by going through all symptoms row
            newSymptoms = []
            for x in range(0, 5):
                if row["SYMPTOM%d"%(x+1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (row["SYMPTOM%d"%(x+1)], row["SYMPTOMVERSION%d"%(x+1)])))
            # Create a Covid Object with all the data
            for x in range(0, 5):
                del row['SYMPTOM%d'%(x+1)]
                del row['SYMPTOMVERSION%d'%(x+1)]
            obj = row
            obj["Symptoms"] = newSymptoms
            hashMap[vaersId] = obj
        # Check if highest number of symptoms needs to be updated
        obj = hashMap[vaersId]
        if len(obj["Symptoms"]) > highestNumOfSymptoms:
            highestNumOfSymptoms = len(obj["Symptoms"])
            idOfMostSymptoms = vaersId

    # Let's convert this HashMap of JSONs into a dataframe --> CSV
    jsonForTask1 = []
    for key in hashMap:
        # For every JSON object, we want to loop through the symptoms and create a more flattened JSON
        obj = hashMap[key]
        for count, symptom in enumerate(obj["Symptoms"], start=1):
            obj["SYMPTOM%d"%(count)] = symptom['SymptomName']
            obj["SYMPTOMVERSION%d"%(count)] = symptom['SymptomVersion']
        del obj["Symptoms"]
        jsonForTask1.append(obj)
    # Convert to Dataframe and then create and save CSV file (Task 1)
    task1Df = pd.DataFrame(jsonForTask1)
    task1Df.to_csv('VAERS_COVID_DataAugust2021.csv', index=False)


    # print("Highest number of symptoms is: " + str(highestNumOfSymptoms))
    # print("VAERS ID with the highest number of symptoms: " + str(idOfMostSymptoms))
    # print("The right number of highest symptoms: " + str(len(hashMap[1400623]["Symptoms"])))
    # print(hashMap[902418])


    # # Trim down to the fields needed for Task 2
    # task2Fields = ['VAERS_ID', 'AGE_YRS', 'SEX', 'VAX_NAME', 'RPT_DATE', 'SYMPTOM1', 'SYMPTOMVERSION1', 'SYMPTOM2',
    #                'SYMPTOMVERSION2', 'SYMPTOM3', 'SYMPTOMVERSION3', 'SYMPTOM4', 'SYMPTOMVERSION4',
    #                'SYMPTOM5', 'SYMPTOMVERSION5', 'DIED', 'DATEDIED', 'SYMPTOM_TEXT']
    # Task1SetTrimmed = pd.read_csv("CombinedDataSet.csv", usecols=task2Fields, low_memory=True,
    #                               encoding="ISO-8859-1", engine='python')
    #
    # # Dictionary(HashMap) to store data
    # hashMap = {}
    #
    # # Let's keep track of the highest number of symptoms (for header creation) [46]
    # highestNumOfSymptoms = 0
    # idOfMostSymptoms = 0
    #
    # # Iterate through the trimmed set and add/update the hashmap
    # for index, row in Task1SetTrimmed.iterrows():
    #     id = row["VAERS_ID"]
    #     # If ID is already in the HashMap, just update the symptoms for it
    #     if id in hashMap.keys():
    #         # Update the existing object with the additional symptoms
    #         obj = hashMap[id]
    #         finalSymptoms = obj.symptoms
    #         # Go through all symptoms and create a new symptoms array
    #         newSymptoms = []
    #         if pd.notna(row["SYMPTOM1"]):
    #             newSymptoms.append(SymptomObject(row["SYMPTOM1"], row["SYMPTOMVERSION1"]))
    #         if pd.notna(row["SYMPTOM2"]):
    #             newSymptoms.append(SymptomObject(row["SYMPTOM2"], row["SYMPTOMVERSION2"]))
    #         if pd.notna(row["SYMPTOM3"]):
    #             newSymptoms.append(SymptomObject(row["SYMPTOM3"], row["SYMPTOMVERSION3"]))
    #         if pd.notna(row["SYMPTOM4"]):
    #             newSymptoms.append(SymptomObject(row["SYMPTOM4"], row["SYMPTOMVERSION4"]))
    #         if pd.notna(row["SYMPTOM5"]):
    #             newSymptoms.append(SymptomObject(row["SYMPTOM2"], row["SYMPTOMVERSION5"]))
    #         # Append the arrays and update the hashmap
    #         finalSymptoms.extend(newSymptoms)
    #         obj.symptoms = finalSymptoms
    #         hashMap[id] = obj
    #     # If ID is not in Hashmap, create a new entry
    #     else:
    #         # Create an array of symptoms by going through all symptoms row
    #         symptoms = []
    #         if pd.notna(row["SYMPTOM1"]):
    #             symptoms.append(SymptomObject(row["SYMPTOM1"], row["SYMPTOMVERSION1"]))
    #         if pd.notna(row["SYMPTOM2"]):
    #             symptoms.append(SymptomObject(row["SYMPTOM2"], row["SYMPTOMVERSION2"]))
    #         if pd.notna(row["SYMPTOM3"]):
    #             symptoms.append(SymptomObject(row["SYMPTOM3"], row["SYMPTOMVERSION3"]))
    #         if pd.notna(row["SYMPTOM4"]):
    #             symptoms.append(SymptomObject(row["SYMPTOM4"], row["SYMPTOMVERSION4"]))
    #         if pd.notna(row["SYMPTOM5"]):
    #             symptoms.append(SymptomObject(row["SYMPTOM2"], row["SYMPTOMVERSION5"]))
    #         # Create a Covid Object with all the data
    #         obj = CovidObject(row["AGE_YRS"], row["SEX"], row["VAX_NAME"], row["RPT_DATE"], symptoms, row["DIED"],
    #                           row["DATEDIED"], row["SYMPTOM_TEXT"])
    #         # Add object to the hashmap (dictionary)
    #         hashMap[id] = obj
    #     # Check if highest number of symptoms needs to be updated
    #     obj = hashMap[id]
    #     if len(obj.symptoms) > highestNumOfSymptoms:
    #         highestNumOfSymptoms = len(obj.symptoms)
    #         idOfMostSymptoms = id

    # # Print to test the hashmap
    # testing = hashMap[1400623]
    # print(testing.symptoms)
    # print("\n")
    # print(testing.died)
    # print("Highest number of symptoms is: " + str(highestNumOfSymptoms))
    # print("VAERS ID with the highest number of symptoms: " + str(idOfMostSymptoms))
    # print("The right number of highest symptoms: " + str(len(hashMap[1400623].symptoms)))

    # "AGE_YRS": self.age,
    # "SEX": self.sex,
    # "VAX_NAME": self.vaxName,
    # "RPT_DATE": self.rptDate,
    # "SYMPTOMS": self.symptoms.to_dict(),
    # "DIED": self.died,
    # "DATEDIED": self.dateDied,
    # "SYMPTOM_TEXT": self.symptomText,

    # list_of_rows = [{'AGE_YRS': hashMap[data].age,
    #                     "SEX": hashMap[data].sex,
    #                     "VAX_NAME": hashMap[data].vaxName,
    #                     "RPT_DATE": hashMap[data].rptDate,
    #                     "SYMPTOMS": hashMap[data].symptoms,
    #                     "DIED": hashMap[data].died,
    #                     "DATEDIED": hashMap[data].dateDied,
    #                     "SYMPTOM_TEXT": hashMap[data].symptomText} for data in hashMap]
    # df = pd.DataFrame(list_of_rows)
    # df.to_csv('SYMPTOMDATA.csv', index=False)

    # print(pd.DataFrame.from_dict(hashMap,orient='index'))

    #     print(row["VAERS_ID"], row["SYMPTOM_TEXT"])

    # print(Task1SetTrimmed.loc[0,:])

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
# --> THIRD, convert this hashmap we have into a csv file somehow, lol good luck. Save it too
# --> FOURTH, create the 3 sorting algorithms to sort that hashmap with the ID :)) [Don't use CSV for it I think]


# checking empty stuff yo
# for index, row in Task1SetTrimmed.iterrows():
#     # if np.isnan(row["SYMPTOM5"]):
#     if pd.isna(row["SYMPTOM5"]):
#         print("SYMPTOM 5 is " + str(row["SYMPTOM5"]))
