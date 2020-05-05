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

station = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])
station.createOrReplaceTempView("station")


match_zip = spark.sql("SELECT * \
                        FROM covid \
                        WHERE `ZIPCODE` in (SELECT `zipcode` FROM station) \
                        ORDER BY `DATE`, `ZIPCODE`")

match_zip.select(format_string("%s,%d,%d,%d,%.2f",date_format(match_zip["`DATE`"],'yyyy-MM-dd') \
       ,match_zip["`ZIPCODE`"],match_zip["`POSITIVE`"],match_zip["`TOTAL`"],match_zip["`RATE`"])) \
       .write.save("covid19_with_matching_zipcode.out",format="text")
