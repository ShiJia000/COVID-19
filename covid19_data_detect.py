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

#find min of positive cases
min_positive = spark.sql("SELECT * FROM covid ORDER BY `POSITIVE` ASC LIMIT 10")
min_positive.select(format_string("%s,%d,%d,%d,%.2f",date_format(min_positive["`DATE`"],'yyyy-MM-dd'),min_positive["`ZIPCODE`"],min_positive["`POSITIVE`"],min_positive["`TOTAL`"],min_positive["`RATE`"])) \
       .write.save("min_positive.out",format="text")

#find max of positive cases
max_positive = spark.sql("SELECT * FROM covid ORDER BY `POSITIVE` DESC LIMIT 10")
max_positive.select(format_string("%s,%d,%d,%d,%.2f",date_format(max_positive["`DATE`"],'yyyy-MM-dd'),max_positive["`ZIPCODE`"],max_positive["`POSITIVE`"],max_positive["`TOTAL`"],max_positive["`RATE`"])) \
       .write.save("max_positive.out",format="text")

#check illegal rate
#-- rate is greater than 100
check_rate = spark.sql("SELECT * FROM covid WHERE `RATE` > 100")
check_rate.coalesce(1).rdd.map(lambda x: ((x[0],x[1]),(x[2],x[3],[4]))).saveAsTextFile("illgeal_rate.out")
#-- rate is null or not a number
check_rate2 = spark.sql("SELECT `RATE` FROM covid ORDER BY `RATE` DESC")
check_rate2.coalesce(1).rdd.map(lambda x: (x[0])).saveAsTextFile("illgeal_rate2.out")

#find how many dates in the data
num_dates = spark.sql("SELECT DISTINCT `DATE`, count(*) as num FROM covid GROUP BY `DATE` ORDER BY `DATE`")
num_dates.select(format_string("%s,%d",date_format(num_dates["`DATE`"],'yyyy-MM-dd'),num_dates["num"])) \
        .write.save("num_dates.out",format="text")

#find how many zipcodes in the data
num_codes = spark.sql("SELECT DISTINCT `ZIPCODE`, count(*) as num FROM covid GROUP BY `ZIPCODE` ORDER BY `ZIPCODE`")
num_codes.coalesce(1).rdd.map(lambda x: (x[0],x[1])).saveAsTextFile("num_codes.out")

sc.stop()