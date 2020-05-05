import csv
import datetime
import sys

with open(sys.argv[2],'w') as outfile:
	with open(sys.argv[1]) as infile:
		readablefile = csv.reader(infile)
		count=0
		for row in readablefile:
			writablefile = csv.writer(outfile)
			if count==0:
				writablefile.writerow(['ZIPCODE','DATE','POSITIVE','TOTAL','RATE','DIFF'])
				count = 1
				pre_data = [row[0],row[1],row[2],row[3],row[4]]
				writablefile.writerow([row[0],row[1],row[2],row[3],row[4], 0])
				continue

			if row[0] != pre_data[0]:
				pre_data = [row[0],row[1],row[2],row[3],row[4]]
				writablefile.writerow([row[0],row[1],row[2],row[3],row[4], 0])

			else:
				diff = float(row[4]) - float(pre_data[-1])
				pre_data = [row[0],row[1],row[2],row[3],row[4]]

				writablefile.writerow([row[0],row[1],row[2],row[3],row[4], diff])
