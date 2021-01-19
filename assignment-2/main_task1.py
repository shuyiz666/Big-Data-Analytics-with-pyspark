#!/usr/bin/env python
# coding: utf-8


from __future__ import print_function
import sys
import re
import numpy as np

from numpy import dot
from numpy.linalg import norm
from pyspark import SparkContext


sc = SparkContext(appName="A2")



wikiCategoryLinks=sc.textFile(sys.argv[2])

wikiCats=wikiCategoryLinks.map(lambda x: x.split(",")).map(lambda x: (x[0].replace('"', ''), x[1].replace('"', '') ))

wikiPages = sc.textFile(sys.argv[1])


numberOfDocs = wikiPages.count()


validLines = wikiPages.filter(lambda x : 'id' in x and 'url=' in x)


keyAndText = validLines.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6])) 



def buildArray(listOfIndices):
    
    returnVal = np.zeros(20000)
    
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    
    mysum = np.sum(returnVal)
    
    returnVal = np.divide(returnVal, mysum)
    
    return returnVal


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


keyAndText = validLines.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))



regex = re.compile('[^a-zA-Z]')

keyAndListOfWords = keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))

allWords = keyAndListOfWords.flatMap(lambda x:((i,1) for i in x[1]))
allWords.take(10)

allCounts = allWords.reduceByKey(lambda a, b: a + b)

topWords = allCounts.top(20000, lambda x: x[1])

topWordsK = sc.parallelize(range(20000))

dictionary = topWordsK.map (lambda x : (topWords[x][0], x))


allWordsWithDocID = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))

allDictionaryWords = dictionary.join(allWordsWithDocID)

justDocAndPos = allDictionaryWords.map(lambda x: (x[1][1], x[1][0]))

sc.stop()
