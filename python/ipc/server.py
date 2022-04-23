import sys
import socket
import pyarrow as pa
import pyarrow.dataset as ds


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
                with pa.ipc.new_stream(sink, batch.schema) as writer:
                    writer.write_batch(batch)
                sink.close()
                buf = sink.getvalue()
                print(buf.size)
                conn.sendall(buf.to_pybytes())
                
    print("Done sending batches")
