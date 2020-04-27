import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import date_format
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
spark = SparkSession.builder.appName("t1").getOrCreate()

Subway = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
Subway.createOrReplaceTempView("Subway")

data = spark.sql("SELECT s.`TIME`, s.`UNIT`, s.`SCP`, s.`STATION`, s.`LINENAME`, s.`DATE`, s.`ENTRIES`, s.`EXITS` \
				FROM Subway s, \
					(SELECT Min(`TIME`) as ti, `UNIT`, `SCP`, `STATION`, `LINENAME`, `DATE` \
					FROM Subway \
					GROUP BY `UNIT`, `SCP`, `STATION`, `LINENAME`, `DATE`) as temp \
				WHERE s.`TIME` = temp.ti \
				AND temp.`UNIT` = s.`UNIT` \
				AND temp.`SCP` = s.`SCP` \
				AND temp.`STATION` = s.`STATION` \
				AND temp.`LINENAME` = s.`LINENAME` \
				AND temp.`DATE` = s.`DATE` \
				ORDER BY s.`UNIT`, s.`SCP`, s.`STATION`, s.`LINENAME`, s.`DATE`, s.`TIME`")

data.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s",data["s.`UNIT`"],data["s.`SCP`"],data["s.`STATION`"], \
		data["s.`LINENAME`"],data["s.`DATE`"],data["s.`TIME`"],data["s.`ENTRIES`"], data["s.`EXITS`"])) \
		.write.save("subway_data_sql.out",format="text")
