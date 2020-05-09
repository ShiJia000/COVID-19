import sys
from csv import reader
from pyspark import SparkContext


sc = SparkContext()

income = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x))
positive = sc.textFile(sys.argv[2]).mapPartitions(lambda x: reader(x))
mta = sc.textFile(sys.argv[3]).mapPartitions(lambda x: reader(x))


positive_data = positive.filter(lambda x: x[0] != 'DATE').map(lambda x: (int(x[1]), x[2])).reduceByKey(lambda x, y: int(x) + int(y))

income_data = income.filter(lambda x: x[0] != 'state').map(lambda x: (int(x[1]), x[7]))

mta_data = mta.map(lambda x: (int(x[0]), x[1]))

# income join positive case and 
income_positive_mta = positive_data.join(income_data).join(mta_data)

# zipcode, positive case, income, decrease rate
output = income_positive_mta.map(lambda x: str(x[0]) + ',' + str(x[1][0][0]) + ',' + str(x[1][0][0]) + ',' + str(x[1][1]))

output.saveAsTextFile("covid19_mta_income_relation.out")