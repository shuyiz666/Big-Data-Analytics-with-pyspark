from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime
import numpy as np

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

    sc = SparkContext(appName="Task4")
    #taxi-data-sorted-small.csv.bz2
    lines = sc.textFile(sys.argv[1], 1)

    taxis = lines.map(lambda x: x.split(','))
    texilinesCorrected = taxis.filter(correctRows).filter(MissingData) # remove missing data

    # 1 cash or card?
    cash = texilinesCorrected.filter(lambda x: x[10] == 'CSH')
    cash_percentage = round(cash.count()/texilinesCorrected.count()*100,2)
    card = texilinesCorrected.filter(lambda x: x[10] == 'CRD')
    card_percentage = round(card.count()/texilinesCorrected.count()*100,2)
    output_card = ['%s%%'%(cash_percentage)+' taxi customers pay with cash, '+'%s%%'%(card_percentage)+' taxi customers pay with card.']
    # print('%s%%'%(cash_percentage)+' taxi customers pay with cash')
    # print('%s%%'%(card_percentage)+' taxi customers pay with card')
    sc.parallelize(output_card).coalesce(1).saveAsTextFile(sys.argv[2])

    hour_card = card.map(lambda x: (datetime.strptime(x[2],"%Y-%m-%d %H:%M:%S").hour,1)).reduceByKey(add)
    hour = texilinesCorrected.map(lambda x: (datetime.strptime(x[2],"%Y-%m-%d %H:%M:%S").hour,1)).reduceByKey(add)
    output_hour_card = hour_card.join(hour).map(lambda x: (x[0],'%s%%'%round(x[1][0]/x[1][1]*100,2)))
    # print('hour: the percentage of pay with card')
    # for (hour, percentage) in output_hour_card:
    #     print('%s:%s%%' % (hour, round(percentage*100,2)))
    output_hour_card.saveAsTextFile(sys.argv[2])

    # 2 efficiency of drivers
    money = texilinesCorrected.map(lambda x:(x[1],float(x[16]))).reduceByKey(add)
    distance = texilinesCorrected.map(lambda x:(x[1],float(x[5]))).reduceByKey(add)
    driver_money_distance = money.join(distance)
    output_efficiency = driver_money_distance.map(lambda x: (x[0], round(x[1][0] / x[1][1],2))).top(10,lambda x:x[1])
    # for (driver, money_per_mile) in output_efficiency:
    #     print('%s:%f' % (driver, round(money_per_mile,2)))
    sc.parallelize(output_efficiency).coalesce(1).saveAsTextFile(sys.argv[2])

    # 3 tip amount
    tip = texilinesCorrected.map(lambda x:float(x[14]))
    tiplist = tip.collect()
    # print('%s:%f' % ('mean', np.mean(tiplist)))
    # print('%s:%f' % ('median', np.median(tiplist)))
    Q1 = np.percentile(tiplist,25)
    Q3 = np.percentile(tiplist,75)
    # print('%s:%f' % ('first quantiles', Q1))
    # print('%s:%f' % ('third quantiles', Q3))
    tip.coalesce(1).saveAsTextFile(sys.argv[2])
    sc.parallelize(Q1).coalesce(1).saveAsTextFile(sys.argv[2])
    sc.parallelize(Q3).coalesce(1).saveAsTextFile(sys.argv[2])

    # 4 outliers
    IQR = Q3 - Q1
    lower_range = float(Q1 - (1.5 * IQR))
    upper_range = float(Q3 + (1.5 * IQR))

    outliers_lower = tip.filter(lambda x: x < lower_range).map(lambda x: (x,lower_range-x))
    outliers_upper = tip.filter(lambda x: x > upper_range).map(lambda x: (x,x-upper_range))
    outliers = outliers_lower.union(outliers_upper)
    output_outliers = outliers.top(10, lambda x:x[1])
    # print('top10 outliers are:')
    # for (outliers, gap) in output_outliers:
    #     print(outliers)
    sc.parallelize(output_outliers).coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()
