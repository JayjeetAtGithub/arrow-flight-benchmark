import imp


import time

import pyarrow.dataset as ds


if __name__ == "__main__":
    dataset_ = ds.dataset("/mnt/data/flight_dataset", format="parquet")
    for i in range(10):
        s = time.time()
        table = dataset_.to_table(use_threads=False)
        e = time.time()
        print(e - s)
