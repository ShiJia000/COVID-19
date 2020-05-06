import sys
from csv import reader
from pyspark import SparkContext
import datetime

sc = SparkContext()

rdd = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x))

def group_by_date(v):
	key = v[1]

	# entrance + exits
	value = int(v[2]) + int(v[3])
	return (key, value)

# change key to yesterday
def yest_num(v):
	# date time object
	dt_obj = datetime.datetime.strptime(v[0], "%Y-%m-%d")
	# timestamp
	dt_ts_int = int(datetime.datetime.timestamp(dt_obj) + 24 * 60 * 60)
	dt_ts = datetime.datetime.fromtimestamp(dt_ts_int)

	key = dt_ts.strftime("%Y-%m-%d")
	value = v[1]

	return(key, value);

# only need data in 2020.02
def data_filter(v):
	dt_obj = datetime.datetime.strptime(v[1], "%Y-%m-%d")
	dt_ts_int = int(datetime.datetime.timestamp(dt_obj))

	if dt_ts_int >= 1580446800:
		return True

	return False

# the num of people who use subway in NYC for each day
daily_num = rdd.filter(data_filter).map(group_by_date).reduceByKey(lambda x, y: x + y)
			# .map(lambda x: x[0] + ',' + str(x[1]))

yest_daily_num = daily_num.map(yest_num)

output = daily_num.join(yest_daily_num) \
		.map(lambda x: (x[0], x[1][0] - x[1][1])) \
		.sortByKey() \
		.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("turnstile_nyc_daily_change.out")