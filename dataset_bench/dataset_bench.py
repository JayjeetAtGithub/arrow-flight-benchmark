import gc
import time

import pyarrow.dataset as ds


if __name__ == "__main__":
    for i in range(10):
        s = time.time()
        dataset_ = ds.dataset("/mnt/data/flight_dataset", format="parquet")
        table = dataset_.to_table(use_threads=False)
        e = time.time()
        print(e - s)

        del table
        del dataset_
        gc.collect()
