#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function
import sys
import re
import numpy as np
import pandas as pd
from numpy import dot
from numpy.linalg import norm
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import functions as func
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf, expr, concat, col, split, explode, lit, monotonically_increasing_id, array, size, sum as sum_, pandas_udf, PandasUDFType
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="A2_4")
sqlContext = SQLContext(sc)

schema =  StructType([
    StructField('DocID', StringType(),True),
    StructField('Category', StringType(),True)
])

wikiCats = sqlContext.read.format('csv').options(header='false', infoerSchema = 'true', sep=',').load(sys.argv[2], schema = schema)
wikiPages = sqlContext.read.format('csv').options(sep='|').load(sys.argv[1])

numberOfDocs = wikiPages.count()

validLines = wikiPages.filter(wikiPages._c0.like("%id%")).filter(wikiPages._c0.like("%url=%"))
get_ID = udf(lambda x: x[x.index('id="') + 4 : x.index('" url=')], StringType())
get_text = udf(lambda x: x[x.index('">') + 2:][:-6],StringType())
keyAndText = validLines.withColumn('DocID',get_ID(validLines._c0)).withColumn('Text',get_text(validLines._c0)).drop('_c0')


def buildArray(listOfIndices):
    
    returnVal = np.zeros(20000)
    
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    
    mysum = np.sum(returnVal)
    
    returnVal = np.divide(returnVal, mysum)
    
    return returnVal.tolist()


def build_zero_one_array (listOfIndices):
    
    returnVal = np.zeros (20000)
    
    for index in listOfIndices:
        if returnVal[index] == 0: returnVal[index] = 1
    
    return returnVal


def stringVector(x):
    returnVal = str(x[0])
    for j in x[1]:
        returnVal += ',' + str(j)
    return returnVal



def cousinSim (x,y):
	normA = np.linalg.norm(x)
	normB = np.linalg.norm(y)
	return np.dot(x,y)/(normA*normB)


regex = re.compile('[^a-zA-Z]')
remove_nonletter = udf(lambda x : regex.sub(' ', x).lower().split(), StringType())
keyAndListOfWords = keyAndText.withColumn('Text',remove_nonletter(keyAndText.Text))


allWords = keyAndListOfWords.select(explode(split(keyAndListOfWords.Text,',')))
remove_nonletter2 = udf(lambda x : regex.sub(' ', x).lstrip(), StringType())
allWords = allWords.withColumn('words',remove_nonletter2(allWords.col)).withColumn('COUNT',lit(1)).drop('col')


allCounts = allWords.groupBy("words").agg(func.sum("COUNT"))
topWords = allCounts.orderBy("sum(COUNT)", ascending=False).limit(20000)

dictionary = topWords.withColumn("position",monotonically_increasing_id()).drop("sum(count)")

allWordsWithDocID = keyAndListOfWords.select(explode(split(keyAndListOfWords.Text,',')),keyAndListOfWords.DocID)
allWordsWithDocID = allWordsWithDocID.withColumn('col',remove_nonletter2(allWordsWithDocID.col))

allDictionaryWords = dictionary.join(allWordsWithDocID, allWordsWithDocID.col == dictionary.words,'inner').drop('col')

justDocAndPos = allDictionaryWords.select('DocID','position')

allDictionaryWordsInEachDoc = justDocAndPos.groupBy('DocID').agg(func.collect_list(func.col('position')).alias('position_list'))



buildArray_udf = udf(lambda x: buildArray(x),ArrayType(FloatType()))
allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.withColumn('position_array',buildArray_udf('position_list')).drop('position_list')
print(allDocsAsNumpyArrays.take(3))

zero_one_udf = udf(lambda x: np.clip(np.multiply(np.array(x), 9e50), 0, 1).tolist(),ArrayType(FloatType()))
zeroOrOne = allDocsAsNumpyArrays.withColumn('position_array', zero_one_udf('position_array'))

def aggregate_ndarray(x,y):
    return np.add(x,y)
dfArray = zeroOrOne.select("position_array").rdd.fold([0]*20000, lambda x,y: aggregate_ndarray(x,y))[0]

multiplier = np.full(20000, numberOfDocs)
idfArray = np.log(np.divide(np.full(20000, numberOfDocs), dfArray))

tfidf = udf(lambda x: np.multiply(np.array(x), idfArray).tolist(), ArrayType(FloatType()))
allDocsAsNumpyArraysTFidf = allDocsAsNumpyArrays.withColumn('position_array',tfidf(allDocsAsNumpyArrays.position_array).alias('tfidf'))
print(allDocsAsNumpyArraysTFidf.take(2))

features = wikiCats.join(allDocsAsNumpyArraysTFidf, wikiCats.DocID == allDocsAsNumpyArraysTFidf.DocID,'inner').select("Category","position_array")


def getPrediction (textInput, k):
    text_list = textInput.split(' ')
    data = []
    for word in text_list:
        data.append((regex.sub(' ', word).lower(),1))
    wordsInThatDoc = sqlContext.createDataFrame(data, ["word", "count"])
    allDictionaryWordsInThatDoc = dictionary.join(wordsInThatDoc, wordsInThatDoc.word == dictionary.words,'inner')
    myArray_idf = allDictionaryWordsInThatDoc.groupBy(wordsInThatDoc.word).agg(sum_("count").alias("count"),buildArray_udf(func.collect_list("position")).alias("idf")).orderBy("count",ascending=False).limit(1).select('idf')
    myArray = np.multiply(myArray_idf.select('idf').collect(), idfArray)
    distance_udf = udf(lambda x: float(np.dot(np.array(x), myArray[0][0])),FloatType())
    distances = features.withColumn('distance',distance_udf('position_array'))
    topK = distances.orderBy('distance', ascending=False).limit(k)
    docIDRepresented = topK.withColumn('cnt',lit(1)).drop('tfidf')
    numTimes = docIDRepresented.groupBy("Category").agg(sum_("cnt").alias("count")).drop("cnt")
    numTimes_order = numTimes.orderBy('count', ascending=False).limit(k)
    return numTimes_order.collect()


print(getPrediction('Sport Basketball Volleyball Soccer', 10))


print(getPrediction('What is the capital city of Australia?', 10))


print(getPrediction('How many goals Vancouver score last year?', 10))

sc.stop()

