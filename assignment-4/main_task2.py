#!/usr/bin/env python
# coding: utf-8



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

def Gradient_Descent_LR(data,regression_coefficients,LearningRate,iterations):
    for i in range(iterations):
        pre = regression_coefficients.copy()
        if i:
            pre_loss = loss_regularized.copy()
        data_theta = training_data.map(lambda x: (x[0],x[1],np.dot(x[1],regression_coefficients)))
        loss = data_theta.map(lambda x: -x[0]*x[2]+np.log(1+np.exp(x[2]))).reduce(add)
        
        L2_Regularization = np.sum(regression_coefficients**2)**(1/2)
        loss_regularized = loss+L2_Regularization
        
        gradients = data_theta.map(lambda x:  (-x[1]*x[0]  + x[1]*(np.exp(x[2]) / (1+np.exp(x[2]))))).reduce(add)
        regression_coefficients -= LearningRate*gradients
        
        if i:
            if loss_regularized < pre_loss:
                LearningRate *= 1.05
            else:
                LearningRate *= 0.5

        if i==iterations-1 or (i and (np.linalg.norm(np.subtract(pre , regression_coefficients)) < 0.0001)):
            return regression_coefficients


if __name__ == "__main__":

    sc = SparkContext(appName="Assignment4_task2")

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

    train_doc = train_keyAndListOfWords.map(lambda x:x[0]).collect()
    training_data = allDocsAsNumpyArraysTFidf.filter(lambda x:x[0] in train_doc).map(lambda x: (1 if x[0][:2]== 'AU' else 0,x[1]))
    training_data.cache()



    regression_coefficients = np.full(20000, 0.01)
    LearningRate = 0.1
    iterations=400
    final_coefficients = Gradient_Descent_LR(training_data,regression_coefficients,LearningRate,iterations)



    index_top5 = final_coefficients.argsort()[-5:][::-1]


    words_top5 = dictionary.filter(lambda x:x[1] in index_top5).map(lambda x:(x[0],final_coefficients[x[1]]))
    words_top5.coalesce(1, shuffle = False).saveAsTextFile(sys.argv[3])

    sc.stop()



