import sys
import pandas as pd
import os

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("error")
        exit(-1)

    this_path = os.path.abspath(os.path.dirname(__file__))
    in_path = os.path.join(this_path, sys.argv[1])
    out_path = os.path.join(this_path, sys.argv[2])

    df = pd.read_csv(sys.argv[1], delimiter=',')
    df.to_csv(sys.argv[2])
