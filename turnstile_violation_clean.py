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


##############
# Data Clean #
##############

# remove useless columns
res = spark.sql("SELECT `UNIT`, `SCP`, `STATION`, `LINENAME`, `DATE`, `TIME`, `DESC`, `ENTRIES`, `EXITS`\
							FROM turnstile")
res.createOrReplaceTempView("res")


# Remove table title
res = spark.sql("SELECT * FROM res \
					WHERE `UNIT` <> 'UNIT'")
res.createOrReplaceTempView("res")


# Key collision Regular & Recover data
collision_records = spark.sql("SELECT COUNT(1) AS collision_count, `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							FROM res \
							GROUP BY `UNIT`, `STATION`, `SCP`, `DATE`, `TIME` \
							HAVING collision_count > 1")
collision_records.createOrReplaceTempView("collision_records")


# Check Collision Data
check_collision = spark.sql("SELECT r.* \
								FROM res r\
								JOIN collision_records cr\
									ON r.`UNIT` = cr.`UNIT` \
										AND r.`STATION` = cr.`STATION` \
										AND r.`SCP` = cr.`SCP` \
										AND r.`DATE` = cr.`DATE` \
										AND r.`TIME` = cr.`TIME`")
check_collision.createOrReplaceTempView("check_collision")

check_collision.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s,%s", \
							check_collision["r.`UNIT`"], \
							check_collision["r.`SCP`"], \
							check_collision["r.`STATION`"], \
							check_collision["r.`LINENAME`"], \
							check_collision["r.`DATE`"], \
							check_collision["r.`TIME`"], \
							check_collision["r.`DESC`"], \
							check_collision["r.`ENTRIES`"], \
							check_collision["r.`EXITS`"])) \
		.write.save("check-collision.out", format="text")

# Remove the RECOVR AUD if collision occurs
res = spark.sql("SELECT r.* FROM res r \
			EXCEPT \
				SELECT cr.* FROM check_collision cr \
					WHERE cr.`DESC` = 'RECOVR AUD'" )

res.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s,%s", \
							res["r.`UNIT`"], \
							res["r.`SCP`"], \
							res["r.`STATION`"], \
							res["r.`LINENAME`"], \
							res["r.`DATE`"], \
							res["r.`TIME`"], \
							res["r.`DESC`"], \
							res["r.`ENTRIES`"], \
							res["r.`EXITS`"])) \
		.write.save("turnstile_violation_clean.out", format="text")
res.createOrReplaceTempView("res")