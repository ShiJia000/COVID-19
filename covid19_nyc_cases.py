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

#data of positive cases of NYC 
NYC_cases = spark.sql("SELECT `DATE`, SUM(`POSITIVE`) as sum_positive, SUM(`TOTAL`) as sum_total, \
                SUM(`POSITIVE`)/SUM(`TOTAL`)*100 as rate \
                FROM covid \
                Group by `DATE` \
                ORDER BY `DATE`")

NYC_cases.select(format_string("%s,%d,%d,%.2f",date_format(NYC_cases["`DATE`"],'yyyy-MM-dd'), \
                     NYC_cases["sum_positive"],NYC_cases["sum_total"],NYC_cases["rate"])) \
                 .write.save("covid19_nyc_cases.out",format="text")