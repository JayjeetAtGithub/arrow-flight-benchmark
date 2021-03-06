import sys
import time

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.dataset as ds
import pyarrow.parquet as pq

if __name__ == "__main__":
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    dataset_path = str(sys.argv[3])

    for i in range(0, 10):
        location = flight.Location.for_grpc_tcp(host, port)
        client = pa.flight.connect(location.uri.decode())
        flight_descriptor = pa.flight.FlightDescriptor.for_path(dataset_path)
        flight_info = client.get_flight_info(flight_descriptor)
        # print(flight_info.schema)
        s = time.time()
        reader = client.do_get(flight_info.endpoints[0].ticket)
        read_table = reader.read_all()
        e = time.time()
        #print(read_table.to_pandas())
        print(e - s)
