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

# Detect

# Distinct

# # Distinct stations
# distinct_station = spark.sql("SELECT STATION, COUNT(1) AS num FROM turnstile GROUP BY station ORDER BY STATION")
# distinct_station.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("distinct-station.out")

# # Distinct Date & Time
# distinct_date = spark.sql("SELECT DISTINCT(`date`) FROM turnstile GROUP BY `date` ORDER BY `date`")
# distinct_date.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-date.out")

# # Distinct Time
# distinct_time = spark.sql("SELECT DISTINCT(`time`) FROM turnstile GROUP BY `time` ORDER BY `time`")
# distinct_time.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-time.out")

# # Distinct DESC
# distinct_desc = spark.sql("SELECT DISTINCT(`DESC`) FROM turnstile GROUP BY `DESC` ORDER BY `DESC`")
# distinct_desc.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-desc.out")

# # Distinct DIVISION
# distinct_division = spark.sql("SELECT DISTINCT(`DIVISION`) FROM turnstile GROUP BY `DIVISION` ORDER BY `DIVISION`")
# distinct_division.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-division.out")



# key collision
distinct_key = spark.sql("SELECT `DATE`, `TIME`, `STATION`, `C/A`, `UNIT`, `SCP`, `DIVISION`, COUNT(1) AS collision_count \
							FROM turnstile \
							GROUP BY `C/A`, `UNIT`, `SCP`, `STATION`, `DIVISION`, `DATE`, `TIME` \
							ORDER BY `collision_count`, `DATE`, `TIME`, `STATION`, `C/A`, `UNIT`, `SCP`, `DIVISION`")
# `C/A`,`UNIT`, `SCP`, `STATION`, `LINENAME`, `DIVISION`, `DATE`, `TIME`
distinct_key.coalesce(1).rdd.map(lambda x: ','.join(x[0:7]) + ',' + str(x[7])).saveAsTextFile("distinct-key.out")

# Find the date cannot be ordered, format the data to 'YYYY-MM-DD'

# Find that they did not upload the data in same time.


# Data Clean

# Remove the data of 2019
