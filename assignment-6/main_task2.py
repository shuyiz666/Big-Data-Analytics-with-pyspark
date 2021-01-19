from __future__ import print_function
import sys

from operator import add
from re import sub, search
import numpy as np
from numpy.random.mtrand import dirichlet, multinomial
from string import punctuation
import random
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="a6_t2")
    lines = sc.textFile(sys.argv[1])
    stripped = lines.map(lambda x: sub("<[^>]+>", "", x))
    
    # count most frequent words (top 20000)
    def correctWord(p):
        if (len(p) > 2):
            if search("([A-Za-z])\w+", p) is not None:
                return p

    counts = lines.map(lambda x : x[x.find(">") : x.find("</doc>")]) .flatMap(lambda x: x.split()).map(lambda x : x.lower().strip(".,<>()-[]:;?!")) .filter(lambda x : len(x) > 1) .map(lambda x: (x, 1)).reduceByKey(add)

    sortedwords = counts.takeOrdered(20000, key = lambda x: -x[1])
    top20000 = []
    for pair in sortedwords:
        top20000.append(pair[0])

    # A function ti generate the wourd count. 
    def countWords(d):
        try:
            header = search('(<[^>]+>)', d).group(1)
        except AttributeError:
            header = ''
        d = d[d.find(">") : d.find("</doc>")]
        words = d.split(' ')
        numwords = {}
        count = 0
        for w in words:
            if search("([A-Za-z])\w+", w) is not None:
                w = w.lower().strip(punctuation)
                if (len(w) > 2) and w in top20000:
                    count += 1
                    idx = top20000.index(w)
                    if idx in numwords:
                        numwords[idx] += 1
                    else:
                        numwords[idx] = 1
        return (header, numwords, count)

    def map_to_array(mapping):
        count_lst = [0] * 20000
        i = 0
        while i < 20000:
            if i in mapping:
                count_lst[i] = mapping[i]
            i+= 1
        return np.array(count_lst)

    # calculate term frequency vector for each document
    result = lines.map(countWords)
    result.cache()
    result1 = result.filter(lambda x:x[0][:43]=='<doc id="20_newsgroups/comp.graphics/37261"').map(lambda x:x[1]).collect()[0]

    task1 = {}
    for key, value in result1.items():
        if key < 100:
            task1[key] = value
            
    print(task1)
    
    sc.stop()
