import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

# read data
sc = SparkContext()
spark = SparkSession.builder.appName("covid").config("spark.some.config.option", "some-value").getOrCreate()

# Load Data
turnstile = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
turnstile.createOrReplaceTempView("turnstile")

###############
# Data Detect #
###############


# ENTRIES find min and max value
entries_min = spark.sql("SELECT `ENTRIES` FROM turnstile ORDER BY `ENTRIES` LIMIT 10")
entries_min.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_detect_entries_min.out")

entries_max = spark.sql("SELECT `ENTRIES` FROM turnstile ORDER BY `ENTRIES` DESC LIMIT 10")
entries_max.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_detect_entries_max.out")

# EXITS find min and max value
exits_min = spark.sql("SELECT `EXITS` FROM turnstile ORDER BY `EXITS` LIMIT 10")
exits_min.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_detect_exits_min.out")

exits_max = spark.sql("SELECT `EXITS` FROM turnstile ORDER BY `EXITS` DESC LIMIT 10")
exits_max.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_detect_exits_max.out")

# Distinct

# Distinct stations Check
distinct_station = spark.sql("SELECT STATION, COUNT(1) AS num FROM turnstile GROUP BY station ORDER BY STATION")
distinct_station.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("turnstile_distinct_station.out")

# Distinct Date Check
distinct_date = spark.sql("SELECT DISTINCT(`date`) FROM turnstile GROUP BY `date` ORDER BY `date`")
distinct_date.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_distinct_date.out")

# Distinct Time Check
distinct_time = spark.sql("SELECT DISTINCT(`time`) FROM turnstile GROUP BY `time` ORDER BY `time`")
distinct_time.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_distinct_time.out")

# Distinct DESC Check
distinct_desc = spark.sql("SELECT DISTINCT(`DESC`) FROM turnstile GROUP BY `DESC` ORDER BY `DESC`")
distinct_desc.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_distinct_desc.out")

# Distinct DIVISION Check
distinct_division = spark.sql("SELECT DISTINCT(`DIVISION`) FROM turnstile GROUP BY `DIVISION` ORDER BY `DIVISION`")
distinct_division.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("turnstile_distinct_division.out")

# key collision
distinct_key = spark.sql("SELECT COUNT(1) AS collision_count , `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							FROM turnstile \
							GROUP BY `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							HAVING collision_count > 1")
distinct_key.coalesce(1).rdd.map(lambda x: str(x[0]) + ',' + ','.join(x[1:6])).saveAsTextFile("turnstile_distinct_key.out")
