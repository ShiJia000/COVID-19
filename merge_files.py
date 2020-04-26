import sys
import os

path = 'SubwayData/'
file_list = os.listdir(path)

if __name__ == "__main__":
    with open(sys.argv[1],'w') as outfile:
    	for name in sorted(file_list):
    		with open(path+name) as infile:
    			outfile.write(infile.read())
    		outfile.write("\n")
