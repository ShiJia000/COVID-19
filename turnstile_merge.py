import sys
import os
import glob

path = 'datasets_raw/'
file_list = glob.glob(path + 'turnstile_*.txt')

if __name__ == "__main__":
    # add relative path
    this_path = os.path.abspath(os.path.dirname(__file__))
    out_path = os.path.join(this_path, sys.argv[1])

    with open(out_path,'w') as outfile:
        for name in sorted(file_list):

            print("Reading from " + name)
            with open(name) as infile:
                for l in infile:
                    # Remove spaces at the beginning and at the end of the line
                    outfile.write(l.strip())
                    outfile.write("\n")

    print("All the contents are saved to " + sys.argv[1])