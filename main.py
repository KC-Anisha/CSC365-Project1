import copy
import json

import time
import pandas as pd
from functools import reduce
import random


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


def task1(hashMap):
    hashMapCopy = copy.deepcopy(hashMap)
    jsonForTask1 = []
    for key in hashMapCopy:
        # For every JSON object, we want to loop through the symptoms and create a more flattened JSON
        obj = hashMapCopy[key]
        for count, symptom in enumerate(obj["Symptoms"], start=1):
            obj["SYMPTOM%d" % (count)] = symptom['SymptomName']
            obj["SYMPTOMVERSION%d" % (count)] = symptom['SymptomVersion']
        del obj["Symptoms"]
        jsonForTask1.append(obj)
    # Convert to Dataframe and then create and save CSV file
    task1Df = pd.DataFrame(jsonForTask1)
    task1Df.to_csv('VAERS_COVID_DataAugust2021.csv', index=False)


def task2(hashMap):
    hashMapCopy = copy.deepcopy(hashMap)
    jsonForTask2 = []
    # Loop through the keys in HashMap and create a separate JSON object for each symptom
    for key in hashMapCopy:
        obj = hashMapCopy[key]
        for symptom in obj["Symptoms"]:
            tempDict = {}
            tempDict["VAERS_ID"] = key
            tempDict["AGE_YRS"] = obj['AGE_YRS']
            tempDict["SEX"] = obj['SEX']
            tempDict["VAX_NAME"] = obj['VAX_NAME']
            tempDict["RPT_Date"] = obj['RPT_DATE']
            tempDict["SYMPTOM"] = symptom['SymptomName']
            tempDict["DIED"] = obj['DIED']
            tempDict["DATEDIED"] = obj['DATEDIED']
            tempDict["SYMPTOM_TEXT"] = obj['SYMPTOM_TEXT']
            jsonForTask2.append(json.loads(json.dumps(tempDict)))
    # Convert to Dataframe and then create and save CSV file
    task2Df = pd.DataFrame(jsonForTask2)
    task2Df.to_csv('SYMPTOMDATA.csv', index=False)
    return jsonForTask2


def quicksort(arr, first, last):
    if first < last:
        pivot = partition(arr, first, last)
        quicksort(arr, first, pivot - 1)
        quicksort(arr, pivot + 1, last)


def partition(arr, first, last):
    pivot = first + random.randrange(last - first + 1)
    swap(arr, pivot, last)

    for i in range(first, last):
        if arr[i]["VAERS_ID"] <= arr[last]["VAERS_ID"]:
            swap(arr, i, first)
            first += 1

    swap(arr, first, last)
    return first


def swap(A, x, y):
    A[x], A[y] = A[y], A[x]


def insertionSort(arr):
    for i in range(1, len(arr)):
        key = arr[i]
        # Move elements of array with a greater VAERS_ID than key to one position ahead of their current position
        j = i - 1
        while j >= 0 and key["VAERS_ID"] < arr[j]["VAERS_ID"]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key


def mergeSort(arr):
    if len(arr) > 1:
        mid = len(arr) // 2
        L = arr[:mid]
        R = arr[mid:]
        mergeSort(L)
        mergeSort(R)
        # Traversal iterators
        i = 0
        j = 0
        # Main list iterator
        k = 0
        # Copy data to temp arrays L[] and R[]
        while i < len(L) and j < len(R):
            if L[i]["VAERS_ID"] < R[j]["VAERS_ID"]:
                arr[k] = L[i]
                i += 1
            else:
                arr[k] = R[j]
                j += 1
            k += 1
        # Checking if any element was left
        while i < len(L):
            arr[k] = L[i]
            i += 1
            k += 1
        while j < len(R):
            arr[k] = R[j]
            j += 1
            k += 1


def testQuickSort(arr, arrSize):
    testArray = arr[1:arrSize+1]
    random.shuffle(testArray)
    tic = time.perf_counter()
    quicksort(testArray, 0, len(testArray) - 1)
    toc = time.perf_counter()
    print(f"Quick sorted {arrSize} items in {toc - tic:0.4f} seconds")


def doQuickSort(array):
    arr = copy.deepcopy(array)

    # Quicksort on 50
    testQuickSort(arr, 50)

    # Quicksort on 500
    testQuickSort(arr, 500)

    # Quicksort on 5,000
    testQuickSort(arr, 5000)

    # Quicksort on 10,000
    testQuickSort(arr, 10000)

    # Quicksort on 100,000
    testQuickSort(arr, 100000)

    # Quicksort the entire dataset
    tic = time.perf_counter()
    quicksort(arr, 0, len(arr) - 1)
    toc = time.perf_counter()
    print(f"Quick sorted the entire set in {toc - tic:0.4f} seconds")

    # Create a CSV of the entire quick sorted set
    tic = time.perf_counter()
    pdObj = pd.DataFrame(arr)
    pdObj.to_csv('quickSort.csv', index=False)
    toc = time.perf_counter()
    print(f"Created the quick sorted file in {toc - tic:0.4f} seconds")


def testMergeSort(arr, arrSize):
    testArray = arr[1:arrSize + 1]
    random.shuffle(testArray)
    tic = time.perf_counter()
    mergeSort(testArray)
    toc = time.perf_counter()
    print(f"Merge sorted {arrSize} items in {toc - tic:0.4f} seconds")


def doMergeSort(array):
    arr = copy.deepcopy(array)

    # Merge Sort on 50
    testMergeSort(arr, 50)

    # Merge Sort on 500
    testMergeSort(arr, 500)

    # Merge Sort on 5,000
    testMergeSort(arr, 5000)

    # Merge Sort on 10,000
    testMergeSort(arr, 10000)

    # Merge Sort on 100,000
    testMergeSort(arr, 100000)

    # Merge Sort the entire dataset
    tic = time.perf_counter()
    mergeSort(arr)
    toc = time.perf_counter()
    print(f"Merge sorted the entire set in {toc - tic:0.4f} seconds")

    # Create a CSV of the entire Merge sorted set
    tic = time.perf_counter()
    pdObj = pd.DataFrame(arr)
    pdObj.to_csv('mergeSort.csv', index=False)
    toc = time.perf_counter()
    print(f"Created the Merge sorted file in {toc - tic:0.4f} seconds")


def testInsertionSort(arr, arrSize):
    testArray = arr[1:arrSize + 1]
    random.shuffle(testArray)
    tic = time.perf_counter()
    insertionSort(testArray)
    toc = time.perf_counter()
    print(f"Insertion sorted {arrSize} items in {toc - tic:0.4f} seconds")


def doInsertionSort(array):
    arr = copy.deepcopy(array)

    # Merge Sort on 50
    testInsertionSort(arr, 50)

    # Merge Sort on 500
    testInsertionSort(arr, 500)

    # Merge Sort on 5,000
    testInsertionSort(arr, 5000)

    # Merge Sort on 10,000
    testInsertionSort(arr, 10000)

    # Merge Sort on 100,000
    testInsertionSort(arr, 100000)

    # Merge Sort the entire dataset
    tic = time.perf_counter()
    insertionSort(arr)
    toc = time.perf_counter()
    print(f"Insertion sorted the entire set in {toc - tic:0.4f} seconds")

    # Create a CSV of the entire Merge sorted set
    tic = time.perf_counter()
    pdObj = pd.DataFrame(arr)
    pdObj.to_csv('insertionSort.csv', index=False)
    toc = time.perf_counter()
    print(f"Created the Insertion sorted file in {toc - tic:0.4f} seconds")


def checkForDeath(patientSex, patientVaccine):
    if lastID != patientRow["VAERS_ID"] or lastID == -1:
        if patientRow["DIED"] == 'Y':
            grouping[patientAge][patientSex][patientVaccine][1] += 1
    return


def checkForVaccineName(patientSex):
    if patientRow['VAX_NAME'] == 'COVID19 (COVID19 (JANSSEN))':
        patientVaccine = 'J&J'
        grouping[patientAge][patientSex][patientVaccine][0].append(patientRow)
        checkForDeath(patientSex, patientVaccine)
    elif patientRow['VAX_NAME'] == 'COVID19 (COVID19 (MODERNA))':
        patientVaccine = 'Moderna'
        grouping[patientAge][patientSex][patientVaccine][0].append(patientRow)
        checkForDeath(patientSex, patientVaccine)
    elif patientRow['VAX_NAME'] == 'COVID19 (COVID19 (PFIZER-BIONTECH))':
        patientVaccine = 'Pfizer'
        grouping[patientAge][patientSex][patientVaccine][0].append(patientRow)
        checkForDeath(patientSex, patientVaccine)
    else:
        patientVaccine = 'Unknown'
        grouping[patientAge][patientSex][patientVaccine][0].append(patientRow)
        checkForDeath(patientSex, patientVaccine)
    return


def checkForGender():
    if patientRow['SEX'] == 'M':
        patientSex = 'Male'
        checkForVaccineName(patientSex)
    elif patientRow['SEX'] == 'F':
        patientSex = 'Female'
        checkForVaccineName(patientSex)
    else:
        patientSex = 'Unknown'
        checkForVaccineName(patientSex)
    return


if __name__ == '__main__':
    # Combine all the datasets (all CSV file) and get all the covid data from them
    tic = time.perf_counter()
    combinedDataSet = combineDataSets()
    toc = time.perf_counter()
    print(f"Combined all datasets in {toc - tic:0.4f} seconds")

    # Convert the data into a JSON format for easier looping
    tic = time.perf_counter()
    covidJsonData = json.loads(pd.DataFrame.to_json(combinedDataSet, orient='records'))
    toc = time.perf_counter()
    print(f"Converted data into JSON in {toc - tic:0.4f} seconds")

    # Dictionary(HashMap) to store data
    hashMap = {}

    # Let's keep track of the highest number of symptoms and its id [113]
    highestNumOfSymptoms = 0
    idOfMostSymptoms = 0

    # covidJsonData = json.loads(pd.read_csv('VAERS_COVID_DataAugust2021.csv').to_json(orient='records'))

    # Loop through the data and store it in a hashmap
    tic = time.perf_counter()
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
                if row["SYMPTOM%d" % (x + 1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (
                            row["SYMPTOM%d" % (x + 1)], row["SYMPTOMVERSION%d" % (x + 1)])))
            # Append the arrays and update the hashmap
            finalSymptoms.extend(newSymptoms)
            obj["Symptoms"] = finalSymptoms
            hashMap[vaersId] = obj
        # If ID is not in Hashmap, create a new entry
        else:
            # Create an array of symptoms by going through all symptoms row
            newSymptoms = []
            for x in range(0, 5):
                if row["SYMPTOM%d" % (x + 1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (
                            row["SYMPTOM%d" % (x + 1)], row["SYMPTOMVERSION%d" % (x + 1)])))
            # Create a Covid Object with all the data
            for x in range(0, 5):
                del row['SYMPTOM%d' % (x + 1)]
                del row['SYMPTOMVERSION%d' % (x + 1)]
            obj = row
            obj["Symptoms"] = newSymptoms
            hashMap[vaersId] = obj
        # Check if highest number of symptoms needs to be updated
        obj = hashMap[vaersId]
        if len(obj["Symptoms"]) > highestNumOfSymptoms:
            highestNumOfSymptoms = len(obj["Symptoms"])
            idOfMostSymptoms = vaersId
    toc = time.perf_counter()
    print(f"Created hashMap in {toc - tic:0.4f} seconds")

    # print("Highest number of symptoms is: " + str(highestNumOfSymptoms))
    # print("VAERS ID with the highest number of symptoms: " + str(idOfMostSymptoms))

    print(len(hashMap))

    # Let's convert this HashMap of JSONs to a CSV file - for task 1
    tic = time.perf_counter()
    task1(hashMap)
    toc = time.perf_counter()
    print(f"Task 1 CSV file created in {toc - tic:0.4f} seconds")

    # Task 2 - Create a trimmed down CSV file where each symptom gets it's own row
    tic = time.perf_counter()
    task2Json = task2(hashMap)
    toc = time.perf_counter()
    print(f"Task 2 CSV file created in {toc - tic:0.4f} seconds")

    # QuickSort
    print("--------------------------------------------------")
    doQuickSort(task2Json)

    # Merge Sort
    print("--------------------------------------------------")
    doMergeSort(task2Json)

    # Insertion Sort
    print("--------------------------------------------------")
    doInsertionSort(task2Json)

    task2Json = json.loads(pd.read_csv('SYMPTOMDATA.csv').to_json(orient='records'))

    # Testing sorts on smaller subsets --> the array is shuffled before feeding it to the sorting alogrithms
    testArray = copy.deepcopy(task2Json)
    print("--------------------------------------------------")
    testQuickSort(testArray, 1000)
    print("--------------------------------------------------")
    testInsertionSort(testArray, 1000)
    print("--------------------------------------------------")
    testMergeSort(testArray, 1000)

    # Task 3 - Group, sort and count number of deaths
    # grouping = {'AgeGroup': {'Gender': {'VaccineName': [[Patient, Patient],DeathCount]}}}
    grouping = {'<1': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                       'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                       'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '1-3': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                        'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                        'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '4-11': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                         'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                         'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '12-18': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '19-30': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '31-40': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '41-50': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '51-60': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '61-70': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '71-80': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                          'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                '>80': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                        'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                        'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}},
                'Unknown': {'Male': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                            'Female': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]},
                            'Unknown': {'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown': [[], 0]}}}
    lastID = -1

    # Let's loop through the dataset - Array of JSON
    tic = time.perf_counter()
    for patientRow in task2Json:
        # Deal with age first - then gender - then vaccine group and deaths
        if patientRow["AGE_YRS"] is None:
            patientAge = 'Unknown'
            checkForGender()
        elif patientRow["AGE_YRS"] < 1:
            patientAge = '<1'
            checkForGender()
        elif 1 <= patientRow["AGE_YRS"] <= 3:
            patientAge = '1-3'
            checkForGender()
        elif 4 <= patientRow["AGE_YRS"] <= 11:
            patientAge = '4-11'
            checkForGender()
        elif 12 <= patientRow["AGE_YRS"] <= 18:
            patientAge = '12-18'
            checkForGender()
        elif 19 <= patientRow["AGE_YRS"] <= 30:
            patientAge = '19-30'
            checkForGender()
        elif 31 <= patientRow["AGE_YRS"] <= 40:
            patientAge = '31-40'
            checkForGender()
        elif 41 <= patientRow["AGE_YRS"] <= 50:
            patientAge = '41-50'
            checkForGender()
        elif 51 <= patientRow["AGE_YRS"] <= 60:
            patientAge = '51-60'
            checkForGender()
        elif 61 <= patientRow["AGE_YRS"] <= 70:
            patientAge = '61-70'
            checkForGender()
        elif 71 <= patientRow["AGE_YRS"] <= 80:
            patientAge = '71-80'
            checkForGender()
        elif patientRow["AGE_YRS"] > 80:
            patientAge = '>80'
            checkForGender()
        else:
            patientAge = 'Unknown'
            checkForGender()
        # Update lastID so we don't count death multiple times for same patient
        lastID = patientRow["VAERS_ID"]

    toc = time.perf_counter()
    print("--------------------------------------------------")
    print(f"Data grouped in {toc - tic:0.4f} seconds")

    # Sort the data groups
    tic = time.perf_counter()
    for ageGroup in grouping:
        age = ageGroup
        for gender in grouping[age]:
            sex = gender
            for vaccine in grouping[age][sex]:
                vaccineName = vaccine
                temp = grouping[age][sex][vaccineName][0]
                quicksort(temp, 0, len(temp) - 1)
                sortedDF = pd.DataFrame(temp)
                print("--------------------------------------------------")
                print("Sorted " + vaccineName + " Vaccine with gender " + sex + " of age " + age + ":")
                print(sortedDF)
    toc = time.perf_counter()
    print(f"Sorted all groups in {toc - tic:0.4f} seconds")

    print("--------------------------------------------------")
    print("Death breakdown by everything")
    print("--------------------------------------------------")
    for ageGroup in grouping:
        age = ageGroup
        for gender in grouping[age]:
            sex = gender
            for vaccine in grouping[age][sex]:
                vaccineName = vaccine
                death = grouping[age][sex][vaccineName][1]
                print("Death count for " + vaccineName + " Vaccine with gender " + sex + " of age " + age + " = " + str(death))

    print("--------------------------------------------------")
    print("Death breakdown by age and gender")
    print("--------------------------------------------------")
    for ageGroup in grouping:
        age = ageGroup
        for gender in grouping[age]:
            sex = gender
            death = 0
            for vaccine in grouping[age][sex]:
                vaccineName = vaccine
                death += grouping[age][sex][vaccineName][1]
            print("Death count for gender " + sex + " of age " + age + " = " + str(death))

    print("--------------------------------------------------")
    print("Death breakdown by vaccine")
    print("--------------------------------------------------")
    # 'J&J': [[], 0], 'Moderna': [[], 0], 'Pfizer': [[], 0], 'Unknown':
    jjDeath = modernaDeath = pfizerDeath = unknownVaccineDeath = 0
    for age in grouping:
        for sex in grouping[age]:
            jjDeath += grouping[age][sex]['J&J'][1]
            modernaDeath += grouping[age][sex]['Moderna'][1]
            pfizerDeath += grouping[age][sex]['Pfizer'][1]
            unknownVaccineDeath += grouping[age][sex]['Unknown'][1]
    print("Death count for J&J = " + str(jjDeath))
    print("Death count for Moderna = " + str(modernaDeath))
    print("Death count for Pfizer = " + str(pfizerDeath))
    print("Death count for Unknown vaccine = " + str(unknownVaccineDeath))

    print("--------------------------------------------------")
    print("Death breakdown by gender")
    print("--------------------------------------------------")
    maleDeath = femaleDeath = unknownGenderDeath = 0
    for age in grouping:
        for vaccine in grouping[age]['Male']:
            maleDeath += grouping[age]['Male'][vaccine][1]
            femaleDeath += grouping[age]['Female'][vaccine][1]
            unknownGenderDeath += grouping[age]['Unknown'][vaccine][1]
    print("Death count for Males = " + str(maleDeath))
    print("Death count for Females = " + str(femaleDeath))
    print("Death count for Unknown Gender = " + str(unknownGenderDeath))

    print("--------------------------------------------------")
    print("Death breakdown by age")
    print("--------------------------------------------------")
    for ageGroup in grouping:
        age = ageGroup
        death = 0
        for gender in grouping[age]:
            sex = gender
            for vaccine in grouping[age][sex]:
                vaccineName = vaccine
                death += grouping[age][sex][vaccineName][1]
        print("Death count for age " + age + " = " + str(death))

    print("--------------------------------------------------")
    totalDeath = 0
    for ageGroup in grouping:
        age = ageGroup
        for gender in grouping[age]:
            sex = gender
            for vaccine in grouping[age][sex]:
                vaccineName = vaccine
                totalDeath += grouping[age][sex][vaccineName][1]
    print("Total deaths = " + str(totalDeath))
