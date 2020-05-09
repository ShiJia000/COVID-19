import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession \
.builder \
.appName("mta_monthly_mean") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

inputx = spark.read.format('csv').options(header="false", inferschema="true").load(sys.argv[1])
inputx.createOrReplaceTempView("zipcodeNumPeoplePerDay")

# _c0: zipcode  _c1: date  _c2: num_people_in  _c3: num_people_out

inputx = spark.sql("select zipcode, year_month, (sub.num_people_in + sub.num_people_out) as total_num \
    from (select _c0 as zipcode, date_format(_c1,'yyyy-MM') as year_month, \
    AVG(_c2) as num_people_in, AVG(_c3) as num_people_out \
    from zipcodeNumPeoplePerDay \
    group by zipcode, year_month) as sub")

inputx.createOrReplaceTempView("inputx")

inputx.select(format_string("%s,%s,%.2f", \
    inputx.zipcode, inputx.year_month, inputx.total_num))\
.write.save("mta_monthly_mean.csv", format="text")