import sys
from csv import reader
from pyspark.sql.functions import *
from pyspark import SparkContext
import string, unicodedata
import re

sc = SparkContext()

rdd = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x)).filter(lambda x: x[0]!='StationID').map(lambda x:(x[5],(x[6], x[7])))

def rm_control(str):
    return "".join(ch for ch in str if unicodedata.category(ch)[0]!="C")

def rm_punc(str):
    exclude = set(string.punctuation)
    return "".join(ch for ch in str if ch not in exclude)

def norma_western(str):
    return unicodedata.normalize('NFD', str).encode('ascii', 'ignore').decode("utf-8")

def clean_borough(value):
    borough = value[1][0]
    if borough == "Bk":
        borough = "Brooklyn"
    elif borough == "M":
        borough = "Manhattan"
    elif borough == "Bx":
        borough = "The Bronx"
    elif borough == "Q":
        borough = "Queen"
    elif borough == "SI":
        borough = "Staten Island"
    return (value[0], (borough, value[1][1]))


def ngram_fingerprint(value, n = 2):
    value1 = value[0].lower()
    value1 = rm_punc(value1)
    value1 = rm_control(value1)
    value1 = value1.replace(' ', '')
    strx = value1
    listx = []
    for i in range(len(strx)-1):
        listx.insert(i, strx[i]+strx[i+1])
    listx = set(listx)
    listx = list(listx)
    listx.sort()
    result = ""
    for item in listx:
        result += item
    return (result, (value[1][0], value[1][1]))

def reduceadd(x, y):
    result = str(x) + str(y)
    result = result.replace(' ', '')
    l = list(result)
    l.sort()
    result = ''.join(l)
    return result

def deletetap(value):
    value = value.replace(' ', '')
    return value

output = rdd.map(clean_borough)

output = output.map(ngram_fingerprint)

output = output.map(lambda x: ((x[0], x[1][0]), x[1][1])).reduceByKey(reduceadd)

output = output.map(lambda x:((x[0][0], x[0][1]), deletetap(x[1]))).sortByKey()

output.map(lambda x: ','.join(x[0]) + ',' + x[1]).saveAsTextFile("station_borough.out")
