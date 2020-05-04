from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

def reduce_add(x, y):
    s1 = str(int(x[0])+int(y[0]))
    s2 = str(int(x[1])+int(y[1]))
    return (s1, s2)

if __name__ == "__main__":
    if len(sys.argv) != 2: # check if the command is valid
        print("Usage: pythonfile <input file> <input file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext()

    # argv[1]: (station, date) , (people_in, people_out)
    station_sample = sc.textFile(sys.argv[1], 1)
    station_sample = station_sample.mapPartitions(lambda x: reader(x))
    station_sample = station_sample.map(lambda x: ((x[2], x[4]), (x[5], x[6])))
    output = station_sample.reduceByKey(reduce_add)
    output = output.sortByKey()
    output = output.map(lambda x: ','.join(x[0]) + ',' + ','.join(x[1]))
    output = output.saveAsTextFile("station_numPeople_per_day_output.csv")

    sc.stop()