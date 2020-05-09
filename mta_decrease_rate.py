from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

def getyear(value):
    return value[0:4]

def cal_rate(value):
    zipcode = value[0]
    _2019 = float(value[1][0])
    _2020 = float(value[1][1])
    rate = (_2019 - _2020) / _2019
    rate = round(rate, 2)
    return (value[0], str(rate))

def str_add(a, b):
    outcome = float(a) + float(b)
    outcome = round(outcome, 2)
    return str(outcome)

if __name__ == "__main__":
    if len(sys.argv) != 2: # check if the command is valid
        print("Usage: pythonfile <input file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext()

    content = sc.textFile(sys.argv[1], 1)
    parts_content = content.mapPartitions(lambda x: reader(x))
    output = parts_content.filter(lambda x: x[1] == '2019-03' or x[1] == '2019-04' or x[1] == '2020-03' or x[1] == '2020-04')
    output = output.map(lambda x: ((x[0], getyear(x[1])), x[2]))
    output = output.reduceByKey(str_add)
    output = output.sortByKey()

    output.map(lambda x: ','.join(x[0]) + ',' + x[1]).saveAsTextFile("zipcode_sumnum_per_year.csv")

    _2019num = output.filter(lambda x: x[0][1] == '2019')
    _2020num = output.filter(lambda x: x[0][1] == '2020')
    _2019num = _2019num.map(lambda x: (x[0][0], x[1]))
    _2020num = _2020num.map(lambda x: (x[0][0], x[1]))
    output = _2019num.join(_2020num)

    output = output.map(cal_rate)
    output = output.sortByKey()
    output.map(lambda x: x[0] + ',' + x[1]).saveAsTextFile("mta_decrease_rate.csv")

    sc.stop()
