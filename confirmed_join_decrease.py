import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 3: # check if the command is valid
        print("Usage: pythonfile <input file> <input file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext()

    # argv[1]: income_join_positive.csv
    # ZIPCODE,POSITIVE_CASE,AVG_INCOME
    income_positive = sc.textFile(sys.argv[1], 1)
    income_positive = income_positive.mapPartitions(lambda x: reader(x))
    income_positive = income_positive.filter(lambda line: line[0] != "ZIPCODE")
    income_positive = income_positive.map(lambda x: (str(x[0]), x[1] + ',' + x[2]))

    # argv[2]: decrease_rate_2019_2020.csv
    # ZIPCODE,DECREASE_RATE
    decrease = sc.textFile(sys.argv[2], 1)
    decrease = decrease.mapPartitions(lambda x: reader(x))
    decrease = decrease.filter(lambda line: line[0] != "ZIPCODE")
    decrease = decrease.map(lambda x: (str(x[0]), x[1]))
    
    # inner join
    output = decrease.join(income_positive)
    output = output.sortByKey()
    output = output.map(lambda x: x[0] + ',' + ','.join(x[1]))
    output = output.saveAsTextFile("confirmed_join_decrease_output.csv")

    sc.stop()
