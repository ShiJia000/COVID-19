from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2: # check if the command is valid
        print("Usage: pythonfile <input file> <input file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext()

    sample = sc.textFile(sys.argv[1], 1)
    sample = sample.mapPartitions(lambda x: reader(x))
    output = sample.filter(lambda line: int(line[2]) > 1000000000)

    output = output.saveAsTextFile("abnormal_output.csv")

    sc.stop()