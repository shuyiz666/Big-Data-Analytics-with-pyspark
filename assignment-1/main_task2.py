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
            if float(p[5])!=0 and float(p[4])!=0 and float(p[11])!=0:
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

    sc = SparkContext(appName="Top-10 Best Drivers")
    #taxi-data-sorted-small.csv.bz2
    lines = sc.textFile(sys.argv[1], 1)

    taxis = lines.map(lambda x: x.split(','))
    texilinesCorrected = taxis.filter(correctRows).filter(MissingData) # remove missing data

    driver_money = texilinesCorrected.map(lambda x: (x[1],float(x[16]))).reduceByKey(add)
    driver_minutes = texilinesCorrected.map(lambda x: (x[1],float(x[4])/60)).reduceByKey(add)
    driver_money_minutes = driver_money.join(driver_minutes)

    output = sc.parallelize(driver_money_minutes.map(lambda x: (x[0],x[1][0]/x[1][1])).top(10, lambda x:x[1]))
    # for (driver, money_per_min) in output:
    #     print('%s:%f' % (driver, money_per_min))
    output.coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()
