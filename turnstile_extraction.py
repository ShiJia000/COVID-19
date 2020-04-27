import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import date_format
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
spark = SparkSession.builder.appName("covid").getOrCreate()

Subway = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
Subway.createOrReplaceTempView("Subway")

data = spark.sql("SELECT s._c5, s._c0, s._c1, s._c2, s._c3, s._c4, s._c7, s._c8 \
				FROM Subway s, \
					(SELECT Min(_c5) as ti, _c0, _c1, _c2, _c3, _c4 \
					FROM Subway \
					GROUP BY _c0, _c1, _c2, _c3, _c4) as temp \
				WHERE s._c5 = temp.ti \
				AND temp._c0 = s._c0 \
				AND temp._c1 = s._c1 \
				AND temp._c2 = s._c2 \
				AND temp._c3 = s._c3 \
				AND temp._c4 = s._c4 \
				ORDER BY s._c0, s._c1, s._c2, s._c3, s._c4, s._c5")

data.select(format_string("%s,%s,%s,%s,%s,%s,%s,%s", \
				data["s._c0"], \
				data["s._c1"], \
				data["s._c2"], \
				data["s._c3"], \
				data["s._c4"], \
				data["s._c5"], \
				data["s._c7"], \
				data["s._C8"])) \
		.write.save("turnstile_extraction.out", format="text")
