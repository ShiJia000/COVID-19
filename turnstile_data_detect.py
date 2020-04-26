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

# Distinct Date
distinct_date = spark.sql("SELECT DISTINCT(`date`) FROM turnstile ORDER BY `date`")
distinct_date.coalesce(1).rdd.saveAsTextFile("distinct-date.out")