import sys
from csv import reader
from pyspark import SparkContext

def formal_output(out):
	key = out[0]
	value1 = out[1][0]
	value2 = out[1][1]
	return ((value2[0],value2[1],key[0],key[1],value2[2],value2[3]),(value1,int(float(value2[4])),int(float(value2[5])))) #station,line,unit,scp,date,time,ent,exit,borough

def sortOutput(x):
	return x[0][0]+','+x[0][1]+','+x[0][2]+','+x[0][3]+','+x[0][4]+','+x[0][5]+','+x[1][0]+','+str(x[1][1])+','+str(x[1][2])

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("error")
		exit(-1)

	sc = SparkContext()

	Station = sc.textFile(sys.argv[1],1)
	Station = Station.mapPartitions(lambda x: reader(x)) \
					.map(lambda x: ((x[0],x[2]),x[1]))

	Subway = sc.textFile(sys.argv[2],1)
	Subway = Subway.mapPartitions(lambda x: reader(x)) \
				   .map(lambda x:((x[2],x[3]), (x[0],x[1],x[4],x[5],x[6],x[7])))

	output = Station.join(Subway)
	output = output.map(formal_output).sortByKey()

	sort_output = output.map(sortOutput)

	sort_output.saveAsTextFile('turnstile_borough_join.out')
	sc.stop()
