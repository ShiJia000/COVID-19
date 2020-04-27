# COVID-19

## Datasets

NY MTA
http://web.mta.info/developers/turnstile.html

MTA Field Description:
http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt

MTA Stations to County:
http://web.mta.info/developers/data/nyct/subway/Stations.csv

### HDFS Datasets:
Turnstile: /user/js11182/turnstile.csv
Station and borough: /user/xj710/station_borough.out

## Run Book
### merge_files.py
```
python3 merge_files.py datasets/turnstile.txt
```

### convert_data_from_txt_to_csv.py
```
python3 convert_data_from_txt_to_csv.py datasets/turnstile.txt datasets/turnstile.csv
```

### detect turnstile

```
module load python/gnu/3.6.5
module load spark/2.4.0 

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
/home/js11182/COVID-19/turnstile_data_detect.py \
/user/js11182/turnstile.csv
```
