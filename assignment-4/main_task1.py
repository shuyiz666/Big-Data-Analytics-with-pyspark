
from __future__ import print_function

import re
import sys
import numpy as np
from operator import add

from pyspark import SparkContext

def freqArray (listOfIndices):
    returnVal = np.zeros (20000)
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    mysum = np.sum(returnVal)
    returnVal = np.divide(returnVal, mysum)
    return returnVal


def datainput(text):
    d_corpus = sc.textFile(text)
    d_keyAndText = d_corpus.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))
    regex = re.compile('[^a-zA-Z]')
    d_keyAndListOfWords = d_keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))
    return d_keyAndListOfWords

if __name__ == "__main__":

    sc = SparkContext(appName="Assignment4_task1")


    train_keyAndListOfWords = datainput(sys.argv[1])
    train_keyAndListOfWords.cache()

    test_keyAndListOfWords = datainput(sys.argv[2])
    test_keyAndListOfWords.cache()

    keyAndListOfWords = train_keyAndListOfWords.union(test_keyAndListOfWords)
    keyAndListOfWords.cache()


    allWords = keyAndListOfWords.flatMap(lambda x:((i,1) for i in x[1]))
    allCounts = allWords.reduceByKey(add)
    topWords = allCounts.top(20000, lambda x: x[1])
    topWordsK = sc.parallelize(range(20000))

    dictionary = topWordsK.map (lambda x : (topWords[x][0], x))
    dictionary.cache()

    allWordsWithDocID = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))
    allDictionaryWords = dictionary.join(allWordsWithDocID)

    justDocAndPos = allDictionaryWords.map(lambda x: (x[1][1], x[1][0]))

    allDictionaryWordsInEachDoc = justDocAndPos.groupByKey()
    allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.map(lambda x: (x[0], freqArray(x[1])))

    zeroOrOne = allDocsAsNumpyArrays.map(lambda x: (x[0],np.clip(np.multiply(x[1], 9e50), 0, 1)))
    dfArray = zeroOrOne.reduce(lambda x1, x2: ("", np.add(x1[1], x2[1])))[1]

    multiplier = np.full(20000, keyAndListOfWords.count())
    idfArray = np.log(np.divide(multiplier, dfArray))

    allDocsAsNumpyArraysTFidf = allDocsAsNumpyArrays.map(lambda x: (x[0], np.multiply(x[1], idfArray)))

    words = ["applicant","and","attack","protein","court"]

    court = []
    wiki = []

    for word in words:
        position = dictionary.filter(lambda x: x[0]==word).map(lambda x:x[1]).collect()[0]
        AU = allDocsAsNumpyArrays.filter(lambda x: x[0][:2]== 'AU').map(lambda x: x[1][position])
        Wiki = allDocsAsNumpyArrays.filter(lambda x: x[0][:2]!= 'AU').map(lambda x: x[1][position])
        AU.cache()
        Wiki.cache()
        CourtAvgTF = AU.reduce(add)/AU.count()
        WikiAvgTF = Wiki.reduce(add)/Wiki.count()
        court.append("average TF value of "+str(word)+" for court documents is "+str(CourtAvgTF))
        wiki.append("average TF value of "+str(word)+" for wikipedia documents is "+str(WikiAvgTF))
    
    result = sc.parallelize([court, wiki])
    result.coalesce(1, shuffle = False).saveAsTextFile(sys.argv[3])
    
    sc.stop()


