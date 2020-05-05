import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
spark = SparkSession \
.builder \
.appName("mean_in_out_top_low10") \
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
.write.save("mean_out_in_2019_2020.csv", format="text")

_2020_2_top10 = spark.sql("select * from inputx \
	where year_month = '2020-02' order by total_num desc limit 10")

_2020_3_top10 = spark.sql("select * from inputx \
	where year_month = '2020-03' order by total_num desc limit 10")

_2020_4_top10 = spark.sql("select * from inputx \
	where year_month = '2020-04' order by total_num desc limit 10")

_2019_2_top10 = spark.sql("select * from inputx \
	where year_month = '2019-02' order by total_num desc limit 10")

_2019_3_top10 = spark.sql("select * from inputx \
	where year_month = '2019-03' order by total_num desc limit 10")

_2019_4_top10 = spark.sql("select * from inputx \
	where year_month = '2019-04' order by total_num desc limit 10")

union_top10 = _2020_2_top10.union(_2020_3_top10).union(_2020_4_top10)\
.union(_2019_2_top10).union(_2019_3_top10).union(_2019_4_top10)

union_top10.createOrReplaceTempView("union_top10")

union_top10.select(format_string("%s,%s,%.2f", \
	union_top10.zipcode, union_top10.year_month, union_top10.total_num))\
.write.save("mean_out_in_top10.csv", format="text")

_2020_2_low10 = spark.sql("select * from inputx \
	where year_month = '2020-02' order by total_num asc limit 10")

_2020_3_low10 = spark.sql("select * from inputx \
	where year_month = '2020-03' order by total_num asc limit 10")

_2020_4_low10 = spark.sql("select * from inputx \
	where year_month = '2020-04' order by total_num asc limit 10")

_2019_2_low10 = spark.sql("select * from inputx \
	where year_month = '2019-02' order by total_num asc limit 10")

_2019_3_low10 = spark.sql("select * from inputx \
	where year_month = '2019-03' order by total_num asc limit 10")

_2019_4_low10 = spark.sql("select * from inputx \
	where year_month = '2019-04' order by total_num asc limit 10")

union_low10 = _2020_2_low10.union(_2020_3_low10).union(_2020_4_low10)\
.union(_2019_2_low10).union(_2019_3_low10).union(_2019_4_low10)

union_low10.createOrReplaceTempView("union_low10")

union_low10.select(format_string("%s,%s,%.2f", \
	union_low10.zipcode, union_low10.year_month, union_low10.total_num))\
.write.save("mean_out_in_low10.csv", format="text")
