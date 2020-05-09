import csv
import datetime

inFile = open("datasets_results/turnstile_sorted.csv", "r")
# inFile = open("a.csv", "r")
reader = csv.reader(inFile)

# new out file
outFile = open('datasets_results/turnstile_daily.csv','w')
writer = csv.writer(outFile)

# threahold
threahold = 40000

# (unit, scp, station, line_name)
turnstile = ["", "", "", ""]
# start date
date = "2018-12-29"
# entrance & exit num
num = ["", ""]

# yesterday daily entrances and exits num used for error num
yest_daily_num = [0, 0]

last_l = []

print("Running...")

for l in reader:
	l_turnstile = [l[0], l[1], l[2], l[3]]
	l_date = l[4]
	l_num = [int(l[6]), int(l[7])]

	# convert date to timestamp
	l_dt = datetime.datetime.strptime(l_date, "%Y-%m-%d")
	dt = datetime.datetime.strptime(date, "%Y-%m-%d")

	l_timestamp = datetime.datetime.timestamp(l_dt)

	timestamp = datetime.datetime.timestamp(dt)

	if turnstile == l_turnstile and l_timestamp - timestamp >= 23 * 60 * 60 and l_timestamp - timestamp <= 25 * 60 * 60:

		# make sure the first date of each turnstile is not calculated
		if num != ["", ""]:
			daily_num = [l_num[0] - num[0], l_num[1] - num[1]]

			# error num
			
			# clean turnstile data for R445,00-00-00,3 AV 138 ST,6. Its cumulative value has been declining.
			if turnstile == ["R445", "00-00-00", "3 AV 138 ST", "6"] \
			or turnstile == ["R425", "00-06-01", "AVENUE U", "F"] \
			or turnstile == ["R408", "00-00-01", "SIMPSON ST", "25"] \
			or turnstile == ["R394", "01-06-01", "BAY PKWY", "N"] \
			or turnstile == ["R220", "01-00-02", "CARROLL ST", "FG"] \
			or turnstile == ["R195", "02-00-00", "161/YANKEE STAD", "BD4"] \
			or turnstile == ["R188", "01-00-03", "50 ST", "CE"] \
			or turnstile == ["R187", "01-00-01", "81 ST-MUSEUM", "BC"] \
			or turnstile == ["R180", "00-03-02", "103 ST", "6"] \
			or turnstile == ["R179", "00-00-0B", "86 ST", "456"] \
			or turnstile == ["R176", "00-00-05", "33 ST", "6"] \
			or turnstile == ["R175", "00-00-00", "8 AV", "ACEL"] \
			or turnstile == ["R146", "00-00-00", "HUNTS POINT AV", "6"] \
			or turnstile == ["R132", "00-00-04", "125 ST", "56"] \
			or turnstile == ["R104", "00-00-00", "167 ST", "BD"] \
			or turnstile == ["R099", "00-00-00", "DEKALB AV", "BDNQR"] \
			or turnstile == ["R053", "00-00-03", "3 AV-149 ST", "25"] \
			or turnstile == ["R046", "00-06-00", "GRD CNTRL-42 ST", "4567S"] \
			or turnstile == ["R045", "00-03-01", "GRD CNTRL-42 ST", "4567S"] \
			or turnstile == ["R022", "00-05-04", "34 ST-HERALD SQ", "BDFMNQRW"] \
			or turnstile == ["R020", "00-00-03", "47-50 STS ROCK", "BDFM"] \
			or turnstile == ["R022", "00-05-03", "34 ST-HERALD SQ", "BDFMNQRW"] \
			or turnstile == ["R132", "00-00-04", "125 ST", "456"] \
			or turnstile == ["R362", "00-00-00", "ALLERTON AV", "25"]:
				if daily_num[0] < 0:
					daily_num[0] = -daily_num[0]
				if daily_num[1] < 0:
					daily_num[1] = -daily_num[1]

				if daily_num[0] > threahold:
					daily_num[0] = yest_daily_num[0]
				if daily_num[1] > threahold:
					daily_num[1] = yest_daily_num[1]

			else:
				# entrance
				if daily_num[0] < 0 or daily_num[0] > threahold:
					daily_num[0] = yest_daily_num[0]

				# exit
				if daily_num[1] < 0 or daily_num[1] > threahold:
					daily_num[1] = yest_daily_num[1]

			# update yesterday data
			yest_daily_num = daily_num

			writer.writerow(l_turnstile + [date] + daily_num)

		# update
		turnstile = l_turnstile
		date = l_date
		num = l_num

	# the same turnstile and the the two dates differ by more than one day
	elif turnstile == l_turnstile and l_timestamp - timestamp > 24 * 60 * 60:

		# use yesterday's num
		if num != ["", ""]:
			daily_num = yest_daily_num
			writer.writerow(l_turnstile + [date] + daily_num)

		# update
		turnstile = l_turnstile
		date = l_date
		num = l_num

	# not the same turnstile
	elif turnstile != l_turnstile:
		# update
		turnstile = l_turnstile
		date = "2018-12-29"
		num = ["", ""]
		yesterday_daily = [0, 0]
		print(".")

print("Done.")

inFile.close()
outFile.close()