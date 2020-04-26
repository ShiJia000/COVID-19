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

# Distinct stations
distinct_station = spark.sql("SELECT STATION, COUNT(1) AS num FROM turnstile GROUP BY station ORDER BY STATION")
distinct_station.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("distinct-station.out")

# Distinct Date & Time
distinct_date = spark.sql("SELECT DISCINCT(`date`) FROM turnstile GROUP BY `date` ORDER BY `date`")
distinct_date.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-date.out")

# Distinct Time
distinct_time= spark.sql("SELECT DISCINCT(`time`) FROM turnstile GROUP BY `time` ORDER BY `time`")
distinct_time.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-time.out")

# Find the date cannot be ordered, format the data to 'YYYY-MM-DD'



# Data Clean

# Remove the data of 2019
