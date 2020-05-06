import sys
from csv import reader
from pyspark import SparkContext


sc = SparkContext()

income = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x))
positive = sc.textFile(sys.argv[2]).mapPartitions(lambda x: reader(x))


positive_data = positive.filter(lambda x: x[0] != 'ZIPCODE').map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: int(x) + int(y))

income_data = income.filter(lambda x: x[0] != 'state').map(lambda x: (x[1], x[7]))

output = positive_data.join(income_data).map(lambda x: str(x[0]) + ',' + str(x[1][0]) + ',' + x[1][1])

output.saveAsTextFile("income_join_positive.out")