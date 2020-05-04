import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession.builder.appName("covid").config("spark.some.config.option", "some-value").getOrCreate()
covid = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
covid.createOrReplaceTempView("covid")

station = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])
station.createOrReplaceTempView("station")


#detect if every staiton has a case num
match_zip = spark.sql("SELECT DISTINCT c.`ZIPCODE`, s.`STATION` \
                        FROM covid c, station s \
                        WHERE c.`ZIPCODE` = s.`zipcode` \
                        ORDER BY c.`ZIPCODE`")
match_zip.coalesce(1).rdd.map(lambda x: (x[0],x[1])).saveAsTextFile("match_zip.out")