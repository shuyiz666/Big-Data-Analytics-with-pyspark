#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from operator import add
import numpy as np
import sys


sc = SparkContext(appName="assignment3_task3")


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



# Task 3 - Fit Multiple Linear Regression using Gradient Descent
def Gradient_Descent_multiple(data,b_current,m_current,learningRate,num_iteration,n):

    x_y_prediction = features_total.map(lambda x: (x[0],x[1],np.dot(m_current,x[0])+b_current))
    
    initCost= x_y_prediction.map(lambda x: (x[1]-x[2])**2).reduce(add)
    
    for i in range(num_iteration):

        if not i:
            oldCost = initCost
        else:
            x_y_prediction = features_total.map(lambda x: (x[0],x[1],np.dot(m_current,x[0])+b_current))
        
        cost = x_y_prediction.map(lambda x: (x[1]-x[2])**2).reduce(add)
            
        # calculate gradients. 
        m_gradient = x_y_prediction.map(lambda x: x[0]*(x[1]-x[2])).reduce(add)*(-2/n)
        b_gradient = x_y_prediction.map(lambda x: x[1]-x[2]).reduce(add)*(-2/n)

        # update the weights - Regression Coefficients 
        m_current -= learningRate * m_gradient
        b_current -= learningRate * b_gradient
        
        # Bold driver
        if cost < oldCost:
            learningRate = 1.05*learningRate
        elif cost > oldCost:
            learningRate = 0.5*learningRate

        oldCost = cost
        
        print("iter:", i+1 , "cost:", cost, "m:", m_current, " b:", b_current) 
        print("m_gradient:", m_gradient, "b_gradient:", b_gradient)
        print("learningRate:", learningRate)


features_total = data.map(lambda x: (np.array([float(x[4])/60,float(x[5]),float(x[11]),float(x[12])]),float(x[16])))
features_total.cache()
m_current = np.array([0.1]*4)
b_current = 0.1
learningRate = 0.001
num_iteration = 100
n= data.count()
Gradient_Descent_multiple(features_total,b_current,m_current,learningRate,num_iteration,n)



sc.stop()

