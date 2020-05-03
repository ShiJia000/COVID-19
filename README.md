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

Turnstile: `/user/js11182/turnstile.csv `

Station Borough: `/user/xj710/stations.csv`

COVID-19: `/user/hz2204/COVID-19_clean.csv`

## Run Book

Go to the COVID-19/ path
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
