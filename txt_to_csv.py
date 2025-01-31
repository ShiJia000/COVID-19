import sys
import pandas as pd
import os

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("error")
        exit(-1)

    this_path = os.path.abspath(os.path.dirname(__file__))

    # input file
    in_path = os.path.join(this_path, sys.argv[1])

    # output file
    out_path = os.path.join(this_path, sys.argv[2])

    # read txt 
    print("Reading from " + sys.argv[1] + "...")
    df = pd.read_csv(in_path, delimiter=',', low_memory=False)
    print("Reading done!")

    # write csv 
    print("Writing to " + sys.argv[2] + "...")
    df.to_csv(out_path)
    print("Writing done!")