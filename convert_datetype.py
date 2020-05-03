
import pandas as pd 
import csv
import datetime
import sys

with open(sys.argv[2],'w') as outfile:
	with open(sys.argv[1]) as infile:  #turnstile.csv
		readablefile = csv.reader(infile)
		count=0
		for row in readablefile:
			writablefile = csv.writer(outfile)
			if count==0:
				writablefile.writerow(row)
				count = 1
				continue

			date_format = datetime.datetime.strptime(row[-5], '%m/%d/%Y').date() #date
			time_format = datetime.datetime.strptime(row[-4], '%H:%M:%S').time() #time
			entr = int(row[-2]) #entries
			exit = int(row[-1]) #exit

			writablefile.writerow([row[0],row[1],row[2],row[3],row[4], \
				row[5],row[6],date_format,time_format,row[-3],entr,exit])

