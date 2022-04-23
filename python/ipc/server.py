import sys
import socket
import pyarrow as pa
import pyarrow.dataset as ds

MSGLEN = 106404536

def mysend(msg, sock):
    totalsent = 0
    while totalsent < MSGLEN:
        sent = sock.send(msg[totalsent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent

if __name__ == '__main__':
    host = str(sys.argv[1])
    port = int(sys.argv[2])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        print(f"Server started at: {host}:{port}")
        s.listen()
        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}")
            
            dataset_ = ds.dataset("/mnt/data/flight_dataset", format="parquet")
            reader = ds.Scanner.from_dataset(dataset_, use_threads=False).to_reader()
            for batch in reader:
                sink = pa.BufferOutputStream()
                with pa.ipc.RecordBatchFileWriter(sink, batch.schema) as writer:
                    writer.write_batch(batch)
                    writer.close()
                buf = sink.getvalue()
                print(buf.size)
                mysend(buf.hex(), conn)
                with pa.ipc.RecordBatchFileReader(buf.hex()) as reader:
                    batch = reader.get_batch(0)
                    print(f"Batch size: {batch.num_rows}") 
   
   print("Done sending batches")
