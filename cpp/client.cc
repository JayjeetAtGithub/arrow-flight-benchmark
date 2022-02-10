#include <iostream>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>


arrow::Result<std::shared_ptr<arrow::flight::FlightClient>> ConnectToFlightServer() {
  arrow::flight::Location location;
  ARROW_RETURN_NOT_OK(
      arrow::flight::Location::ForGrpcTcp("localhost", 33004, &location));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
  std::cout << "Connected to " << location.ToString() << std::endl;
  return client;
}

int main() {
  auto client = ConnectToFlightServer();
  auto descriptor = arrow::flight::FlightDescriptor::Path({"flight_datasets/16MB.uncompressed.parquet"});
  std::cout << "Created flight descriptor" << std::endl;

  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  client->GetFlightInfo(descriptor, &flight_info);
  std::cout << flight_info->descriptor().ToString() << std::endl;

  std::cout << "Got flight info" << std::endl;

  std::cout << "=== Schema ===" << std::endl;
  std::shared_ptr<arrow::Schema> info_schema;
  arrow::ipc::DictionaryMemo dictionary_memo;
  flight_info->GetSchema(&dictionary_memo, &info_schema);
  std::cout << info_schema->ToString() << std::endl;
  std::cout << "==============" << std::endl;

  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  client->DoGet(flight_info->endpoints()[0].ticket, &stream);
  std::shared_ptr<arrow::Table> table;
  stream->ReadAll(&table);
  std::cout << table->ToString() << std::endl;
}
