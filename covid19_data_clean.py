import pandas as pd 
import csv
import datetime
import sys
import os


path = 'datasets/'

im_path = os.path.join(path, sys.argv[1])
out_path = os.path.join(path, sys.argv[2])

with open(out_path,'w') as outfile:
	with open(im_path) as infile:
		readablefile = csv.reader(infile)
		count=0
		for row in readablefile:
			writablefile = csv.writer(outfile)
			if count==0:
				writablefile.writerow(['DATE','ZIPCODE','POSITIVE','TOTAL','RATE'])
				count = 1
				continue
			if row[1] == 'NA' or row[1]=='99999':
				continue
			
			date = row[0].split('T')[0].split('-')
			date_str = '{1:s}/{2:s}/{0:s}'.format(date[0],date[1],date[2])
			date_format = datetime.datetime.strptime(date_str, '%m/%d/%Y').date() #date

			if str(date_format) != '2020-04-26' :
				rate = '{0:.2f}'.format(int(row[2])/int(row[3])*100)
				writablefile.writerow([date_format,row[1],row[2],row[3],rate])
			else:
				rate = '{0:.2f}'.format(int(row[3])/int(row[2])*100)
				writablefile.writerow([date_format,row[1],row[3],row[2],rate])