import sys

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.dataset as ds


class FlightServer(pa.flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._dataset = None

    def _make_flight_info(self, dataset_path):
        self._dataset = ds.dataset(dataset_path, format="parquet")
        schema = self._dataset.schema
        descriptor = pa.flight.FlightDescriptor.for_path(dataset_path.encode('utf-8'))
        endpoints = [pa.flight.FlightEndpoint(dataset_path, [self._location])]
        return pa.flight.FlightInfo(schema, descriptor, endpoints, 0, 0)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def do_get(self, context, ticket):
        reader = ds.Scanner.from_dataset(self._dataset, use_threads=False).to_reader()
        return pa.flight.RecordBatchStream(reader)


if __name__ == '__main__':
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    location = flight.Location.for_grpc_tcp(host, port)
    server = FlightServer(location)
    print(f"Server started at: {location.uri.decode()}")
    server.serve()
