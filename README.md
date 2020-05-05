# COVID-19

Detailed description of our project and data clean part are in <b>[Data_Cleaning.pdf](https://github.com/ShiJia000/COVID-19/blob/master/Data_Cleaning.pdf)</b>. Please read it!!!

## Google Map API

install googlemaps module
```
$ pip install -U googlemaps
```

get the relationship between station and zipcode
```
$ python transfer_zipcode.py
```

## Datasets

[datasets-used.csv](https://github.com/ShiJia000/COVID-19/blob/master/datasets/datasets-used.csv)

NYC MTA
http://web.mta.info/developers/turnstile.html

MTA Field Description:
http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt

MTA Stations to County:
http://web.mta.info/developers/data/nyct/subway/Stations.csv

COVID-19 Positive Cases for each County:
https://github.com/nytimes/covid-19-data

### HDFS Datasets:

Turnstile: `/user/js11182/turnstile.csv`

Station Borough: `/user/xj710/stations.csv`

COVID-19: `/user/hz2204/COVID-19_clean.csv`

Turnstile daily clean data: `/user/js11182/turnstile_daily.csv`

## Run Book

Go to the `COVID-19/` path

### Data Download
Download 'import.sh' which comes from https://github.com/remram44/coronavirus-data (author by remram44)
```
git clone https://github.com/nychealth/coronavirus-data
sh import.sh > datasets/COVID-19.csv
```

### Data Wrangling 

#### merge_files.py
```
python3 merge_files.py datasets/turnstile.txt
```

#### convert_data_from_txt_to_csv.py
```
python3 convert_data_from_txt_to_csv.py datasets/turnstile.txt datasets/turnstile.csv
```

#### sort the turnstile data
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_sort.py \
/user/js11182/turnstile.csv
```

#### run script to transfer culumative data to daily num
```
python3 turnstile_daily_num.py
```
upload this csv data to HDFS(`/user/js11182/turnstile_daily.csv`)
```
cd datasets/
hfs -put turnstile_daily.csv
```

#### join station table with zipcode table
group by (station, date) => output file: station_numPeople_per_day_output.csv
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
/home/xj710/project/station_numPeople_per_day.py \
/user/js11182/turnstile_daily.csv
```
join and group by (zipcode) => output file: station_join_zipcode_output.csv
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
/home/xj710/project/station_join_zipcode.py \
/user/xj710/station_zipcode.csv \
/user/xj710/station_numPeople_per_day_output.csv
```
check abnormal data => output file: abnormal_output.csv
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
/home/xj710/project/data_abmormal_test.py \
/user/xj710/station_join_zipcode_output.csv
```

### Data Detect

#### python & spark version
```
module load python/gnu/3.6.5
module load spark/2.4.0 
```

#### Detect data issues of turnstile

```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_data_detect.py \
/user/js11182/turnstile.csv
```

#### Detect data issues of COVID-19

```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
covid19_data_detect.py \
/user/hz2204/COVID-19_clean.csv
```

#### Detect data issues of COVID-19 and Station (matching zipcode)

```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
covid19_and_station_data_detect.py \
/user/hz2204/COVID-19_clean.csv \
/user/hz2204/station_zipcode.csv
```

### Data Cleaning 

#### Clean the violations in turnstile data
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_violation_clean.py \
/user/js11182/turnstile.csv

hfs -getmerge turnstile_violation_clean.out turnstile_violation_clean.out
hfs -rm -r turnstile_violation_clean.out
hfs -put turnstile_violation_clean.out
```

#### Clean covid-19 data
```
python clean_covid19_data.py COVID-19.csv COVID-19_clean.csv
```

#### Extract the useful data in turnstile
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_extraction.py \
/user/js11182/turnstile_violation_clean.out

hfs -getmerge turnstile_extraction.out turnstile_extraction.out
hfs -rm -r turnstile_extraction.out
hfs -put turnstile_extraction.out
```

#### Station clean in turnstile
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_station_clean.py \
/user/js11182/turnstile_extraction.out

hfs -getmerge turnstile_station_clean.out turnstile_station_clean.out
hfs -rm -r turnstile_station_clean.out
hfs -put turnstile_station_clean.out
```

#### Station borough clean
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
station_borough_clean.py \
/user/xj710/station_borough.csv

hfs -getmerge station_borough_clean.out station_borough_clean.out
hfs rm -r station_borough_clean.out
hfs -put station_borough_clean.out
```

### Join data
```
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
turnstile_borough_join.py \
/user/js11182/station_borough_clean.out \
/user/js11182/turnstile_station_clean.out
```

# 一些有趣的points:
1. 有24个turnstile 倒着计数
2. 有几个turnstile 有规律地跳动
3. 有一些大约40000的  24小时平均每分钟超过20个人[40000是我们的outlier]
4. 研究过程很曲折

# 数据分析 ideas
#### 比较19年和20年的turnstile数据
#### Calculate outlier(in turnstile data) using IQR
#### metadata
#### zipcode, (station1, station2…)
#### data analysis
1. identify potential quality issues
	Null Values
	Find outlier
2. spatial converage
    1. which regions that are data poor
    2. is there missing data for certain regions
    3. how does spatial coverage vary over time
    4. is there a correlation
3. most frequent, max, mean, standard deviation


## Datasets
