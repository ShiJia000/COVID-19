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

- **NYC MTA turnstile:**

  - **[Local] ** Remove spaces at the beginning and at the end of the line. Then **merge the several turnstile datasets and save it to one text(.txt) file**: 【这个马上就好】

    ```shell
    # python3 merge_files.py + output file
    python3 turnstile_merge.py datasets_results/turnstile.txt
    ```

  - **[Local]** Convert the text(.txt) file to a comma-separated values(.csv) file: 【这个需要跑一会】

    ```shell
    # python3 txt_to_csv.py + input file + output file(must be csv)
    python3 txt_to_csv.py datasets_results/turnstile.txt datasets_results/turnstile.csv
    ```

  - **[Dumno]** Detect the issues in turnstile dataset 

    ```shell
    # upload the merged turnstile.csv to HDFS. 
    # turnstile.csv is too big to upload on github, you need to run the previous scripts to get the turnstile.csv. Or you can use the file on HDFS(/user/js11182/turnstile.csv).
    hfs -put datasets_results/turnstile.csv
    
    # run the detection script
    spark-submit --conf \
    spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    turnstile_detect.py \
    /user/js11182/turnstile.csv
    ```

  - **[Dumbo]** Sort the 

    ```shell
    # upload turnstile.csv to HDFS
    hfs -put datasets_results/turnstile.csv
    # 
    ```

    

## Data Analysis



## Data Visulization



## Results



## Challenges

