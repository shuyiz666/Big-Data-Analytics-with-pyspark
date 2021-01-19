from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

def correctRows(p):
    if len(p)==17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[5])!=0 and float(p[11])!=0:
                return p

def MissingData(p):
    cnt = 0
    if len(p)==17:
        for i in range(17):
            if p[i]:
                cnt += 1
    if cnt == 17:
        return p

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top-10 Active Taxis")
    lines = sc.textFile(sys.argv[1], 1)

    taxis = lines.map(lambda x: x.split(','))
    texilinesCorrected = taxis.filter(correctRows).filter(MissingData) # remove missing data
    taxi_driver_count = texilinesCorrected.map(lambda x: (x[0],x[1])).distinct().map(lambda x: (x[0],1)).reduceByKey(add)
    output = sc.parallelize(taxi_driver_count.top(10, lambda x:x[1]))
    output.coalesce(1).saveAsTextFile(sys.argv[2])
    # for (taxi, driver_cnt) in output:
    #     print('%s:%i' % (taxi, driver_cnt))

    sc.stop()
