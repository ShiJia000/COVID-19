import sys
from csv import reader
from pyspark import SparkContext
import string, unicodedata

sc = SparkContext()

rdd = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x)).filter(lambda x: x[1]!='C/A')

def sortline(value):
    value = str(value)
    l = list(value)
    l.sort()
    result = ''.join(l)
    return result

def date_transform(value):
    value = str(value)
    months = value[0:2]
    dates = value[3:5]
    years = value[6:10]
    return years + '-' + months + '-' + dates

def rm_control(str):
    return "".join(ch for ch in str if unicodedata.category(ch)[0]!="C")

def rm_punc(str):
    exclude = set(string.punctuation)
    return "".join(ch for ch in str if ch not in exclude)

def norma_western(str):
    return unicodedata.normalize('NFD', str).encode('ascii', 'ignore').decode("utf-8")

def ngram_fingerprint(value, n = 2):
    value1 = value[2].lower()
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
    return (value[0], (value[1], result, sortline(value[3]), date_transform(value[4]), value[5], value[6], value[7]))

output = rdd.map(ngram_fingerprint)

output.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("turnstile_station_clean.out")

