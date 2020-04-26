# COVID-19

## Datasets

NY MTA
http://web.mta.info/developers/turnstile.html

MTA Field Description:
http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt

MTA Stations to County:
http://web.mta.info/developers/data/nyct/subway/Stations.csv

## Run Book
### merge_files.py
```
python3 merge_files.py datasets/turnstile.txt
```

### convert_data_from_txt_to_csv.py
```
python3 convert_data_from_txt_to_csv.py datasets/turnstile.txt datasets/turnstile.csv
```
