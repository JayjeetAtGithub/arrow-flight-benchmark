#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"

class ParquetStorageService : public arrow::flight::FlightServerBase {
 public:
  explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> fs)
      : fs_(std::move(fs)) {}

  arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                              const arrow::flight::FlightDescriptor& descriptor,
                              std::unique_ptr<arrow::flight::FlightInfo>* info) {
    ARROW_ASSIGN_OR_RAISE(auto file_info, fs_->GetFileInfo(descriptor.path[0]));
    ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
    *info = std::unique_ptr<arrow::flight::FlightInfo>(
        new arrow::flight::FlightInfo(std::move(flight_info)));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket& request,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
    std::string path;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(request.ticket, &path)); 
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
      
    arrow::fs::FileSelector s;
    s.base_dir = std::move(path);
    s.recursive = true;

    arrow::dataset::FileSystemFactoryOptions options;
    ARROW_ASSIGN_OR_RAISE(auto factory, 
      arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
    arrow::dataset::FinishOptions finish_options;
    ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::TableBatchReader batch_reader(*table);
    ARROW_RETURN_NOT_OK(batch_reader.ReadAll(&batches));

    ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(
                                                  std::move(batches), table->schema()));
    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
        new arrow::flight::RecordBatchStream(owning_reader));
  
    return arrow::Status::OK();
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
      const arrow::fs::FileInfo& file_info) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({});
    std::string path = "file://" + file_info.path();
    auto descriptor = arrow::flight::FlightDescriptor::Path({path});

    arrow::flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = path;
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(
        arrow::flight::Location::ForGrpcTcp("localhost", port(), &location));
    endpoint.locations.push_back(location);

    return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
  }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

int main() {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  arrow::flight::Location server_location;
  arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0, &server_location);

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(fs)));
  server->Init(options);
  std::cout << "Listening on port " << server->port() << std::endl;
  while (1);
}
