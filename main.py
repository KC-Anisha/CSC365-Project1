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


def quicksort(aList, first, last):
    if first < last:
        pivot = partition(aList, first, last)
        quicksort(aList, first, pivot - 1)
        quicksort(aList, pivot + 1, last)


def partition(aList, first, last):
    pivot = first + random.randrange(last - first + 1)
    swap(aList, pivot, last)

    for i in range(first, last):
        if aList[i]["VAERS_ID"] <= aList[last]["VAERS_ID"]:
            swap(aList, i, first)
            first += 1

    swap(aList, first, last)
    return first


def swap(A, x, y):
    A[x], A[y] = A[y], A[x]



def insertionSort(arr):
    for i in range(1, len(arr)):
        key = arr[i]
        # Move elements of arr[0..i-1], that are greater than key,
        # to one position ahead of their current position
        j = i - 1
        while j >= 0 and key["VAERS_ID"] < arr[j]["VAERS_ID"]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key


def mergeSort(arr):
    if len(arr) > 1:
        # Finding the mid of the array
        mid = len(arr) // 2
        # Dividing the array elements
        L = arr[:mid]
        # into 2 halves
        R = arr[mid:]
        # Sorting the first half
        mergeSort(L)
        # Sorting the second half
        mergeSort(R)
        i = j = k = 0
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

    tic = time.perf_counter()
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

    # data = json.loads(pd.read_csv('SYMPTOMDATA.csv').to_json(orient='records'))
    # arr = data[0:1000000]
    # print("DOING")
    # insertionSort(arr)
    # print("DONE")
