import sys
import os
import glob

path = 'datasets/'
file_list = glob.glob(path + 'turnstile_*.txt')

if __name__ == "__main__":
	# add relative path
    this_path = os.path.abspath(os.path.dirname(__file__))
    out_path = os.path.join(this_path, sys.argv[1])

    with open(out_path,'w') as outfile:
        for name in sorted(file_list):
            with open(name) as infile:
                outfile.write(infile.read())
            outfile.write("\n")
