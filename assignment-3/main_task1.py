#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from operator import add
import numpy as np
import sys

sc = SparkContext(appName="assignment3_task1")


data = sc.textFile(sys.argv[1])
data = data.map(lambda x: x.split(','))




# Data Clean-up Step
# Remove all taxi rides that are less than 2 min or more than 1 hours
data = data.filter(lambda x: float(x[4])>=120 and float(x[4])<=3600)
# Remove all taxi rides that have ”fare amount” less than 3 dollar or more than 200 dollar
data = data.filter(lambda x: float(x[11])>=3 and float(x[11])<=200)
# Remove all taxi rides that have ”trip distance” less than 1 mile or more than 50 miles
data = data.filter(lambda x: float(x[5])>=1 and float(x[5])<=50)
# Remove all taxi rides that have ”tolls amount” less than 3 dollar
data = data.filter(lambda x: float(x[15])>=3)


# index 5 (this our X-axis) trip distance trip distance in miles
# index 11 (this our Y-axis) fare amount fare amount in dollars
distance_fare = data.map(lambda x: (float(x[5]),float(x[11])))
distance_fare.take(10)

# Task 1 : Simple Linear Regression
xi_sum = distance_fare.map(lambda x:x[0]).reduce(add)
yi_sum = distance_fare.map(lambda x:x[1]).reduce(add)
xiyi_sum = distance_fare.map(lambda x:x[0]*x[1]).reduce(add)
xi2_sum = distance_fare.map(lambda x:x[0]**2).reduce(add)
n = data.count()

m = (n*xiyi_sum - xi_sum*yi_sum)/(n*xi2_sum - xi_sum**2)
b = (xi2_sum*yi_sum - xi_sum*xiyi_sum)/(n*xi2_sum-xi_sum**2)
print(m)
print(b)


sc.stop()

