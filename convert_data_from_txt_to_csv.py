import sys
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("error")
        exit(-1)

    df = pd.read_csv(sys.argv[1], delimiter=',')
    df.to_csv(sys.argv[2])
