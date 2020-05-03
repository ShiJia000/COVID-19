import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import date_format
from pyspark.sql.functions import format_string

sc = SparkContext()
spark = SparkSession.builder.appName("covid").config("spark.some.config.option", "some-value").getOrCreate()
covid = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
covid.createOrReplaceTempView("covid")

#check illegal rate
check_rate = spark.sql("SELECT * FROM covid WHERE `RATE` > 100")
check_rate.coalesce(1).rdd.map(lambda x: ((x[0],x[1]),(x[2],x[3],[4]))).saveAsTextFile("illgeal_rate.out")

#find how many dates in the data
num_dates = spark.sql("SELECT DISTINCT `DATE`, count(*) as num FROM covid GROUP BY `DATE` ORDER BY `DATE`")
num_dates.select(format_string("%s,%d",date_format(num_dates["`DATE`"],'yyyy-MM-dd'),num_dates["num"])) \
        .write.save("num_dates.out",format="text")

#find how many zipcodes in the data
num_codes = spark.sql("SELECT DISTINCT `ZIPCODE`, count(*) as num FROM covid GROUP BY `ZIPCODE` ORDER BY `ZIPCODE`")
num_codes.coalesce(1).rdd.map(lambda x: (x[0],x[1])).saveAsTextFile("num_codes.out")

sc.stop()