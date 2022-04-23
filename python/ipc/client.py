import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import socket

if __name__ == "__main__":
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    dataset_path = str(sys.argv[3])

    for i in range(0, 10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            st = time.time()
            while True:
                data = s.recv(106404536)
                if not data:
                    break
                else:
                    with pa.ipc.RecordBatchStreamReader(data) as reader:
                        batch = reader.read_next_batch()
                        print(f"Batch size: {batch.num_rows}")

            et = time.time()
        print(et - st)
