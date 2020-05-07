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
min_positive = spark.sql("SELECT * FROM covid ORDER BY `Positive` ASC LIMIT 10")
min_positive.select(format_string("%s,%d,%d,%d,%.2f",date_format(min_positive["`date`"],'yyyy-MM-dd'),min_positive["`MODZCTA`"],min_positive["`Positive`"],min_positive["`Total`"],min_positive["`zcta_cum.perc_pos`"])) \
       .write.save("min_positive.out",format="text")

#find max of positive cases
max_positive = spark.sql("SELECT * FROM covid ORDER BY `Positive` DESC LIMIT 10")
max_positive.select(format_string("%s,%d,%d,%d,%.2f",date_format(max_positive["`date`"],'yyyy-MM-dd'),max_positive["`MODZCTA`"],max_positive["`Positive`"],max_positive["`Total`"],max_positive["`zcta_cum.perc_pos`"])) \
       .write.save("max_positive.out",format="text")

#check illegal rate
#-- rate is greater than 100
check_rate = spark.sql("SELECT * FROM covid WHERE `zcta_cum.perc_pos` > 100")
check_rate.coalesce(1).rdd.map(lambda x: ((x[0],x[1]),(x[2],x[3],[4]))).saveAsTextFile("illgeal_rate.out")
#-- rate is null or not a number
check_rate2 = spark.sql("SELECT `zcta_cum.perc_pos` FROM covid ORDER BY `zcta_cum.perc_pos` DESC")
check_rate2.coalesce(1).rdd.map(lambda x: (x[0])).saveAsTextFile("illgeal_rate2.out")

#find how many dates in the data
num_dates = spark.sql("SELECT DISTINCT `date`, count(*) as num FROM covid GROUP BY `date` ORDER BY `date`")
num_dates.select(format_string("%s,%d",date_format(num_dates["`date`"],'yyyy-MM-dd'),num_dates["num"])) \
        .write.save("num_dates.out",format="text")

#find how many zipcodes in the data
num_codes = spark.sql("SELECT DISTINCT `MODZCTA`, count(*) as num FROM covid GROUP BY `MODZCTA` ORDER BY `MODZCTA`")
num_codes.coalesce(1).rdd.map(lambda x: (x[0],x[1])).saveAsTextFile("num_codes.out")

sc.stop()