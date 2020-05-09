# COVID-19

Detailed description 



## Datasets

- **NYC MTA turnstile:** http://web.mta.info/developers/turnstile.html 

  - Data Position: `./datasets_raw/turnstile_*.csv`

  - Data Range: 12/29/2018 - 05/02/2020

  - Fields Description: http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt

- **NYC turnstile zipcode:**

  - Data fetch: run transfer script and save it to `./datasets_raw/`(will talk about it in the following **Fetch Data** part)
  - Field Description:

- **NYC IRS Income by Zipcode:** https://data.world/jonloyens/irs-income-by-zip-code/workspace/file?filename=IRSIncomeByZipCode.csv

  - Data Position: `./datasets_raw/income_by_zipcode.csv`

  - Data Range: 2013

  - Fields Description: 

- **COVID-19 Cases Data:** https://github.com/nytimes/covid-19-data

  - Data fetch: run **shell** and save it to `./datasets_raw/` (will talk about it in the following **Fetch Data** part)
  - Data Range: 04/01/2020 - Now
  - Fields Description: 



## Runing Environment

- Local:
  - macOS 10.14.6
  - Python 3.7.6

- Dumbo:
  - Spark 2.4.0 [`module load spark/2.4.0` ]
  - Python 3.6.5 [`module load python/gnu/3.6.5`]

- Running path: `COVID-19/`

## Fetch Datasets

- **NYC turnstile zipcode:**
- **COVID-19 Cases Data:**



## Data Cleaning

- **NYC MTA turnstile cleaning:**

  1. **[Local] ** Remove spaces at the beginning and at the end of the line. Then **merge the several turnstile datasets and save it to one text(.txt) file**: 【这个script执行比较快】

     ```shell
   # python3 merge_files.py + output file
     python3 turnstile_merge.py datasets_results/turnstile.txt
   ```
  
  2. **[Local]** Convert the text(.txt) file to a comma-separated values(.csv) file: 【这个需要跑一会】
  
      ```shell
     # python3 txt_to_csv.py + input file + output file(must be csv)
     python3 txt_to_csv.py datasets_results/turnstile.txt datasets_results/turnstile.csv
       
     # check if all the lines from the input file are saved to the output file
     wc -l datasets_results/turnstile.txt
        14383736 datasets_results/turnstile.txt
   wc -l datasets_results/turnstile.csv
        14383736 datasets_results/turnstile.csv
    ```
  
  3. **[Dumbo]** Detect the issues in turnstile dataset 
  
   - Find data outliers
     - Find range of each field
     - Find key collision in the dataset
  
     ``` shell
     # upload the merged turnstile.csv to HDFS.
     # turnstile.csv is too big to upload on github. You need to run the previous scripts to get the turnstile.csv. 
     # Uploading the file to HDFS is really time consuming. You can use the file on HDFS(/user/js11182/turnstile.csv).
     hfs -put datasets_results/turnstile.csv
     
     # run the detection script
     spark-submit --conf \
     spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
     turnstile_detect.py \
     /user/js11182/turnstile.csv
     
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
  
  4. **[Dumbo]** Remove violations in turnstile dataset. 
  
     - Remove useless columns
     - Remove key collision
     - Remove table title
  
     ``` shell
     
     ```
  
     
  
  
  
  5. **[Dumbo]** Sort the 
  
  ```shell
  # upload turnstile.csv to HDFS
  hfs -put datasets_results/turnstile.csv
  # 
  ```
  
  

## Data Analysis



## Data Visulization



## Results



## Challenges

