from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime

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

    sc = SparkContext(appName="Best time of the day to Work on Taxi")
    lines = sc.textFile(sys.argv[1], 1)

    taxis = lines.map(lambda x: x.split(','))
    texilinesCorrected = taxis.filter(correctRows).filter(MissingData) # remove missing data

    driver_surcharge = texilinesCorrected.map(lambda x: (datetime.strptime(x[2],"%Y-%m-%d %H:%M:%S").hour,float(x[12]))).reduceByKey(add)
    driver_distance = texilinesCorrected.map(lambda x: (datetime.strptime(x[2],"%Y-%m-%d %H:%M:%S").hour,float(x[5]))).reduceByKey(add)
    driver_profit_ratio = driver_surcharge.join(driver_distance)

    output = driver_profit_ratio.map(lambda x: (x[0],x[1][0]/x[1][1])).sortBy(lambda x:x[1],ascending=False)
    # for (hour, profit_ratio) in output:
    #     print('%s:%f' % (hour, profit_ratio))
    output.coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()
