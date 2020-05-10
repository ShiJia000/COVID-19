# COVID-19



## Datasets

- **NYC MTA turnstile:** http://web.mta.info/developers/turnstile.html 
- Data Position: `./datasets_raw/turnstile_*.csv`
  
- Data Range: 12/29/2018 - 05/02/2020
  
- Fields Description: http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt
  
- **NYC turnstile zipcode:**
  - Data fetch: run transfer script and save it to `./datasets_raw/`(will talk about it in the following **Fetch Data** part)
- **NYC IRS Income by Zipcode:** https://data.world/jonloyens/irs-income-by-zip-code/workspace/file?filename=IRSIncomeByZipCode.csv
- Data Position: `./datasets_raw/income.csv`
- Data Range: 2013
- **COVID-19 Cases Data:** https://github.com/nytimes/covid-19-data

  - Data fetch: run **shell** and save it to `./datasets_raw/` (will talk about it in the following **Fetch Data** part)
  - Data Range: 04/01/2020 - 05/01/2020



## Runing Environment

- Local:
  - macOS 10.14.6
  - Python 3.7.6

- Dumbo:
  - Spark 2.4.0 [`module load spark/2.4.0` ]
  - Python 3.6.5 [`module load python/gnu/3.6.5`]

- Running path: `COVID-19/`



## Fetch Datasets

- **NYC zipcode by MTA station:**

  ```shell
  # Use googlemaps API to find the relationship data of station and zipcode
  # install googlemaps module
  $ pip install -U googlemaps
  
  # transfer txt to csv
  $ python3 txt_to_csv.py datasets_raw/turnstile_200502.txt datasets_results/station_raw.csv
  
  # get the zipcode of each NYC MTA station and save the data to `datasets_raw/zipcode_station.csv`
  $ python3 zipcode_transfer.py
  
  # upload the zipcode_station.csv to HDFS /user/js11182/zipcode_station.csv
  $ hfs -put datasets_raw/zipcode_station.csv
  ```

  

- **COVID-19 Cases Data:**

  ``` shell
  # 'import.sh' comes from https://github.com/remram44/coronavirus-data (author by remram44)
  $ git clone https://github.com/nychealth/coronavirus-data
  $ sh import.sh > datasets_raw/covid19_zipcode.csv
  
  # Because the author of nychealth has deleted the zipcode data you can use the one in github `datasets_raw/covid19_zipcode.csv` or on HDFS /user/js11182/covid19_zipcode.csv
  $ hfs -put datasets_raw/covid19_zipcode.csv
  ```

  

## Data Cleaning

#### NYC MTA turnstile cleaning:

- **Step 1: [Local]** 

  Remove spaces at the beginning and at the end of the line. Then merge the several turnstile datasets and save it to one text(.txt) file

  ``` shell
  # python3 merge_files.py + output file
  $ python3 turnstile_merge.py datasets_results/turnstile.txt
  ```



- **Step 2: [Local]**

  Convert the text(.txt) file to a comma-separated values(.csv) file

  ``` shell
  # python3 txt_to_csv.py + input file + output file(must be csv)
  $ python3 txt_to_csv.py datasets_results/turnstile.txt datasets_results/turnstile.csv
    
  # check if all the lines from the input file are saved to the output file
  $ wc -l datasets_results/turnstile.txt
     14383736 datasets_results/turnstile.txt
  $ wc -l datasets_results/turnstile.csv
     14383736 datasets_results/turnstile.csv
  ```



- **Step 3: [Dumbo Spark]**

  Detect the issues in turnstile dataset 

   - Find data outliers
  - Find range of each field
  - Find key collision in the dataset

  ``` shell
  # upload the merged turnstile.csv to HDFS.
  # turnstile.csv is too big to upload on github. You need to run the previous scripts to get the turnstile.csv. 
  # Uploading the file to HDFS is really time consuming. You can use the file on HDFS(/user/js11182/turnstile.csv).
  $ hfs -put datasets_results/turnstile.csv
  
  # run the detection script
  $ spark-submit --conf \
  spark.pyspark.python=/share/apps/python/3.6.5/bin/python turnstile_detect.py /user/js11182/turnstile.csv
  
  # Check all the output files produced by turnstile_detect.py find the collisions in them
  
  # turnstile_detect_entries_max.out 
  # (The max value is "ENTRIES" They are removed in step 4)
  
  # turnstile_detect_entries_min.out 
  # (The min value is 0000000000. this issue is solved in step 6)
  
  # turnstile_detect_exits_max.out
  # (The max value is "ENTRIES" They are removed in step 4)
  
  # turnstile_detect_exits_min.out
  # (The min value is 0000000000. this issue is solved in step 6)
  
  # turnstile_distinct_date.out
  # (All the dates are in range except a "DATE" value. This issue is cleaned in step 4)
  
  # turnstile_distinct_desc.out
  # (There are three values: "DESC", "RECOVR AUD" and "REGULAR". "DESC" is invalid. It is removed in step 4)
  
  # turnstile_distinct_division.out
  # (There are 7 values: "BMT", "DIVISION", "IND", "IRT", "PTH", "RIT", "SRT". "DIVISION" is invalid. It is removed in step 4)
  
  # turnstile_distinct_key.out
  # (There are several key collisions because of data recover. This issue was solved in turnstile cleaning step 4)
  
  # turnstile_distinct_station.out
  # (All the stations are valid except the "STATION" value. It is removed in step 4)
  
  # turnstile_distinct_time.out
  # (All the time are between 00:00:00 and 23:59:59 except the "TIME" value. It is removed in step 4)
  ```



- **Step 4: [Dumbo Spark]** 

  Remove violations in turnstile dataset. 

  - Remove useless columns
  - Remove key collision
  - Remove table title

  ``` shell
  # run the turnstile turnstile_clean.py
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python turnstile_clean.py /user/js11182/turnstile.csv
  ```



- **Step 5: [Dumbo Spark]** 

  Sort the turnstile data, which is the preparation for next step.

  ``` shell
  # use the data produced from previous step(Step 4) as input file
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python turnstile_sort.py /user/js11182/turnstile_clean.out
  ```



- **Step 6: [Local]** 

  Calculate the daily passenger flow for each turnstile from culumative turnstile data.

  ``` shell
  # Download the sorted turnstile dataset to ./datasets_results/.
  $ hfs -getmerge turnstile_sorted.out ./datasets_results/turnstile_sorted.csv
  
  # Run the script to transfer the culumative data to daily data.
  $ python3 turnstile_daily.py
  
  # upload the cleaned turnstile_daily.csv data to HDFS.
  $ hfs -put datasets_results/turnstile_daily.csv
  ```




#### COVID-19 Cases cleaning:

- **Step 1: [Dumbo Spark]**

  Detect data issues of COVID-19

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python covid19_detect.py /user/js11182/covid19_zipcode.csv
  ```



- **Step 2: [Local]**

  Clean the COVID-19 to solve the previous issues

  > covid19_clean.csv is used to draw the map of COVID-19

  ```shell
  # run script
  $ python3 covid19_clean.py covid19_zipcode.csv covid19_clean.csv
  
  # upload to HDFS (/user/js11182/covid19_clean.csv)
  $ hfs -put datasets_results/covid19_clean.csv
  ```



- **Step 3: [Dumbo Spark]**

  Detect if the covid19 zipcodes and turnstile zipcode are in same range

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python covid19_station_detect.py /user/js11182/covid19_clean.csv /user/js11182/zipcode_station.csv
  ```



#### NYC IRS Income Data Cleaning:

- **Step 1: [Local]**

  Remove those zipcodes that are not in NYC

  ``` shell
  # run script
  $ python3 income_clean.py
  
  # upload result to HDFS
  $ hfs -put datasets_results/income_clean.csv
  ```

  

## Data Analysis

#### MTA

- **Step 1: [Dumbo Spark]** 

  Calculate the daily passenger flow for each station, which is used to save time for the next step.

  ``` shell
  # run the script
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python station_daily.py /user/js11182/turnstile_daily.csv
  ```



- **Step 2: [Dumbo Spark]** 

  Calculate the daily passenger flow for each zipcode. (Clean the zipcodes that are not in NYC).

  ``` shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python zipcode_daily.py /user/js11182/zipcode_station.csv /user/js11182/station_daily.csv
  ```



- **Step 3: [Dumbo Spark]**

  Check abnormal data in `zipcode_daily.csv`

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python zipcode_daily_abmormal_test.py /user/js11182/zipcode_daily.csv
  ```



- **Step 4: [Dumbo Spark]**

  Calculate the daily change of passenger flow in NYC. (From 02/01/2020 to 04/30/2020)

  > mta_daily_change.csv is used to draw the daily change of MTA chart

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python mta_daily_change.py /user/js11182/zipcode_daily.csv
  
  # download the data and draw graph
  $ hfs -getmerge mta_daily_change.out datasets_results/mta_daily_change.csv
  ```

  

- **Step 5: [Dumbo Spark]**

  Calculate the monthly mean of MTA data

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python mta_monthly_mean.py /user/js11182/zipcode_daily.csv
  ```

  

- **Step 6: [Dumbo Spark]**

  Calculate the decreasing rate from 2019/2020 per zipcode

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python mta_decrease_rate.py /user/js11182/mta_monthly_mean.csv
  ```

  

#### COVID-19 Cases

- **Step 1: [Dumbo Spark]**

  Calculate COVID-19 cases in each area(zipcode) of NYC daily change

  ```shell
  $ spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python covid19_nyc_cases.py /user/js11182/covid19_clean.csv
  
  # download the covid19_nyc_cases.out from HDFS to local path.
  $ hfs -getmerge covid19_nyc_cases.out datasets_results/covid19_nyc_cases.csv
  ```



- **Step2: [Local]**

  Calculate the daily change data of COVID-19 cases

  > covid19_nyc_cases_change.csv is used to draw the NYC COVID-19 Positve Rate chart.

  ```shell
  # run script
  $ python3 covid19_nyc_daily_change.py datasets_results/covid19_nyc_cases.csv datasets/covid19_nyc_cases_change.csv
  ```

  

#### Relation among COVID-19 Cases, Income and MTA passenger flow

- **Step 1: [Dumbo Spark]**

  Join 3 datasets together (MTA data, income data and COVID-19 data)

  > covid19_mta_income_relation.csv is used to draw the following charts:
  >
  > - Average Income and MTA Decrease RATE
  > - Average Income and Positive Cases
  > - MTA Decrease Rate and Positive Cases

  ```shell
  $ spark-submit --conf \
  spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
  covid19_mta_income_relation.py \
  /user/js11182/income_clean.csv \
  /user/js11182/covid19_clean.csv \
  /user/js11182/mta_decrease_rate.csv
  ```

