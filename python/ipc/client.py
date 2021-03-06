import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import socket

MSGLEN = 106404536

def myreceive(sock):
    chunks = []
    bytes_recd = 0
    cnt = 0
    while bytes_recd < MSGLEN:
        print("Receiving...", cnt)
        chunk = sock.recv(min(MSGLEN - bytes_recd, 1024 * 1024))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        print(len(chunk))
        bytes_recd = bytes_recd + len(chunk)
        cnt += 1
    return b''.join(chunks)

if __name__ == "__main__":
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    dataset_path = str(sys.argv[3])

    for i in range(0, 10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            st = time.time()
            while True:
                data = myreceive(s)
                print(len(data))
                if not data:
                    break
                else:
                    with pa.ipc.RecordBatchFileReader(data) as reader:
                        batch = reader.get_batch(0)
                        print(f"Batch size: {batch.num_rows}")

            et = time.time()
        print(et - st)
