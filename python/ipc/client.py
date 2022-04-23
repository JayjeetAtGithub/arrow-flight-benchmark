import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import socket

def recvall(sock):
    BUFF_SIZE = 1024 * 1024 # 1 MB
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            # either 0 or end of data
            break
        print('Received: ', len(data))
    return data

if __name__ == "__main__":
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    dataset_path = str(sys.argv[3])

    for i in range(0, 10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            st = time.time()
            while True:
                data = recvall(s)
                if not data:
                    break
                else:
                    with pa.ipc.RecordBatchStreamReader(data) as reader:
                        batch = reader.read_next_batch()
                        print(f"Batch size: {batch.num_rows}")

            et = time.time()
        print(et - st)
