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
                writablefile.writerow(['DATE','POSITIVE','TOTAL','RATE','DIFF'])
                count = 1
                pre_data = [row[0],row[1],row[2],row[3]]
                writablefile.writerow([row[0],row[1],row[2],row[3], 0])
                continue

            diff = float(row[3]) - float(pre_data[-1])
            pre_data = [row[0],row[1],row[2],row[3]]
            writablefile.writerow([row[0],row[1],row[2],row[3],diff])