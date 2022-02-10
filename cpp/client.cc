#include <iostream>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

int main() {
  arrow::flight::Location location;
  ARROW_RETURN_NOT_OK(
      arrow::flight::Location::ForGrpcTcp("localhost", server->port(), &location));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
  std::cout << "Connected to " << location.ToString() << std::endl;

  // Open example data file to upload
  ARROW_ASSIGN_OR_RAISE(std::string airquality_path,
                        FindTestDataFile("airquality.parquet"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::RandomAccessFile> input,
                        fs->OpenInputFile(airquality_path));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(
      parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

  auto descriptor = arrow::flight::FlightDescriptor::Path({"airquality.parquet"});
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

  // Start the RPC call
  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
  ARROW_RETURN_NOT_OK(client->DoPut(descriptor, schema, &writer, &metadata_reader));

  // Upload data
  std::shared_ptr<arrow::RecordBatchReader> batch_reader;
  std::vector<int> row_groups(reader->num_row_groups());
  std::iota(row_groups.begin(), row_groups.end(), 0);
  ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
  int64_t batches = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->Next());
    if (!batch) break;
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    batches++;
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  std::cout << "Wrote " << batches << " batches" << std::endl;


  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ARROW_RETURN_NOT_OK(client->GetFlightInfo(descriptor, &flight_info));
  std::cout << flight_info->descriptor().ToString() << std::endl;
  std::cout << "=== Schema ===" << std::endl;
  std::shared_ptr<arrow::Schema> info_schema;
  arrow::ipc::DictionaryMemo dictionary_memo;
  ARROW_RETURN_NOT_OK(flight_info->GetSchema(&dictionary_memo, &info_schema));
  std::cout << info_schema->ToString() << std::endl;
  std::cout << "==============" << std::endl;

  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  ARROW_RETURN_NOT_OK(client->DoGet(flight_info->endpoints()[0].ticket, &stream));
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(stream->ReadAll(&table));
  std::cout << table->ToString() << std::endl;
}