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

###############
# Data Detect #
###############


# ENTRIES find min and max value
entries_min = spark.sql("SELECT `ENTRIES` FROM turnstile ORDER BY `ENTRIES` LIMIT 10")
entries_min.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("entries-min.out")

entries_max = spark.sql("SELECT `ENTRIES` FROM turnstile ORDER BY `ENTRIES` DESC LIMIT 10")
entries_max.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("entries-max.out")

# EXITS find min and max value
exits_min = spark.sql("SELECT `EXITS` FROM turnstile ORDER BY `EXITS` LIMIT 10")
exits_min.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("exits-min.out")

exits_max = spark.sql("SELECT `EXITS` FROM turnstile ORDER BY `EXITS` DESC LIMIT 10")
exits_max.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("exits-max.out")

# Distinct

# Distinct stations Check
distinct_station = spark.sql("SELECT STATION, COUNT(1) AS num FROM turnstile GROUP BY station ORDER BY STATION")
distinct_station.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("distinct-station.out")

# Distinct Date Check
distinct_date = spark.sql("SELECT DISTINCT(`date`) FROM turnstile GROUP BY `date` ORDER BY `date`")
distinct_date.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-date.out")

# Distinct Time Check
distinct_time = spark.sql("SELECT DISTINCT(`time`) FROM turnstile GROUP BY `time` ORDER BY `time`")
distinct_time.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-time.out")

# Distinct DESC Check
distinct_desc = spark.sql("SELECT DISTINCT(`DESC`) FROM turnstile GROUP BY `DESC` ORDER BY `DESC`")
distinct_desc.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-desc.out")

# Distinct DIVISION Check
distinct_division = spark.sql("SELECT DISTINCT(`DIVISION`) FROM turnstile GROUP BY `DIVISION` ORDER BY `DIVISION`")
distinct_division.coalesce(1).rdd.map(lambda x: x[0]).saveAsTextFile("distinct-division.out")

# key collision
distinct_key = spark.sql("SELECT COUNT(1) AS collision_count , `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							FROM turnstile \
							GROUP BY `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							HAVING collision_count > 1")
distinct_key.coalesce(1).rdd.map(lambda x: str(x[0]) + ',' + ','.join(x[1:6])).saveAsTextFile("distinct-key.out")

# ##############
# # Data Clean #
# ##############

# # remove useless columns
# res = spark.sql("SELECT `UNIT`, `SCP`, `STATION`, `LINENAME`, `DATE`, `TIME`, `DESC`, `ENTRIES`, `EXITS`\
# 							FROM turnstile")
# res.createOrReplaceTempView("res")


# # Remove table title
# res = spark.sql("SELECT * FROM res \
# 					WHERE `UNIT` <> 'UNIT'")
# res.createOrReplaceTempView("res")


# # Key collision Regular & Recover data
# collision_records = spark.sql("SELECT COUNT(1) AS collision_count, `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
# 							FROM res \
# 							GROUP BY `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
# 							HAVING collision_count > 1")
# collision_records.createOrReplaceTempView("collision_records")


# # Check Collision Data
# check_collision = spark.sql("SELECT r.* \
# 								FROM res r\
# 								JOIN collision_records cr\
# 									ON r.`UNIT` = cr.`UNIT` \
# 										AND r.`STATION` = cr.`STATION` \
# 										AND r.`SCP` = cr.`SCP` \
# 										AND r.`DATE` = cr.`DATE` \
# 										AND r.`TIME` = cr.`TIME`")
# check_collision.createOrReplaceTempView("check_collision")

# check_collision.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s,%s", \
# 							check_collision["r.`UNIT`"], \
# 							check_collision["r.`SCP`"], \
# 							check_collision["r.`STATION`"], \
# 							check_collision["r.`LINENAME`"], \
# 							check_collision["r.`DATE`"], \
# 							check_collision["r.`TIME`"], \
# 							check_collision["r.`DESC`"], \
# 							check_collision["r.`ENTRIES`"], \
# 							check_collision["r.`EXITS`"])) \
# 		.write.save("check-collision.out", format="text")

# # Remove the RECOVR AUD if collision occurs
# res = spark.sql("SELECT r.* FROM res r \
# 			EXCEPT \
# 				SELECT cr.* FROM check_collision cr \
# 					WHERE cr.`DESC` = 'RECOVR AUD'" )

# res.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s,%s", \
# 							res["r.`UNIT`"], \
# 							res["r.`SCP`"], \
# 							res["r.`STATION`"], \
# 							res["r.`LINENAME`"], \
# 							res["r.`DATE`"], \
# 							res["r.`TIME`"], \
# 							res["r.`DESC`"], \
# 							res["r.`ENTRIES`"], \
# 							res["r.`EXITS`"])) \
# 		.write.save("res.out", format="text")
# res.createOrReplaceTempView("res")
