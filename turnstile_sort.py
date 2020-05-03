import sys
from csv import reader
from pyspark import SparkContext
import datetime

sc = SparkContext()

rdd = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x)).filter(lambda x: x[1]!='C/A')

# sort turnstile data by UNIT, SCP, STATION, LINE NAME, DATE, TIME
def prduce_key_value(v):
	# format date time
	date_obj = datetime.datetime.strptime(v[7],'%m/%d/%Y')
	date = datetime.datetime.strftime(date_obj,'%Y-%m-%d')

	key = tuple(v[2:6] + [date] + v[8:9])
	value = tuple(v[9:])
	return (key, value)

output = rdd.map(prduce_key_value).sortByKey()

output.map(lambda x: ','.join(x[0]) + ',' + ','.join(x[1])).saveAsTextFile("turnstile_sorted.out")