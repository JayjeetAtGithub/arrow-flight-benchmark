import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import socket

MSGLEN = 1024 * 1024

def myreceive(sock):
    chunks = []
    bytes_recd = 0
    while bytes_recd < MSGLEN:
        chunk = sock.recv(min(MSGLEN - bytes_recd, 2048))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)
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
                data = recvall(s)
                if not data:
                    break
                else:
                    with pa.ipc.RecordBatchStreamReader(data) as reader:
                        batch = reader.read_next_batch()
                        print(f"Batch size: {batch.num_rows}")

            et = time.time()
        print(et - st)
