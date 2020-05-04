from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

def reduce_add(x, y):
    s1 = str(int(x[0])+int(y[0]))
    s2 = str(int(x[1])+int(y[1]))
    return (s1, s2)

if __name__ == "__main__":
    if len(sys.argv) != 3: # check if the command is valid
        print("Usage: pythonfile <input file> <input file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext()

    # argv[1]: station , zipcode
    station_zipcode = sc.textFile(sys.argv[1], 1)
    station_zipcode = station_zipcode.mapPartitions(lambda x: reader(x))
    station_zipcode = station_zipcode.filter(lambda line: line[1] != "STATION")
    station_zipcode = station_zipcode.map(lambda x: (x[1], x[2]))

    # argv[2]: station , date , entries , exits
    station_people = sc.textFile(sys.argv[2], 1)
    station_people = station_people.mapPartitions(lambda x: reader(x))
    station_people = station_people.map(lambda x: (x[1], (x[2], x[3], x[4])))
    
    # inner join
    output = station_zipcode.join(station_people) 
    output = output.map(lambda x: x[1][0] + ',' + ','.join(x[1][1]))
    output = output.mapPartitions(lambda x: reader(x))
    output = output.map(lambda x: ((x[0], x[1]), (x[2], x[3])))
    output = output.reduceByKey(reduce_add)
    output = output.sortByKey()
    output = output.map(lambda x: ','.join(x[0]) + ',' + ','.join(x[1]))
    output = output.saveAsTextFile("test_join_output.csv")

    sc.stop()
