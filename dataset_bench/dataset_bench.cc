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

int main(int argc, char** argv) {
    std::string path;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri("/mnt/data/flight_dataset", &path)); 
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    arrow::fs::FileSelector s;
    s.base_dir = std::move(path);
    s.recursive = true;

    auto fragment_scan_options = std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();
    fragment_scan_options->arrow_reader_properties->set_pre_buffer(true);
    fragment_scan_options->arrow_reader_properties->set_use_threads(true);

    arrow::dataset::FileSystemFactoryOptions options;
    ARROW_ASSIGN_OR_RAISE(auto factory, 
    arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
    arrow::dataset::FinishOptions finish_options;
    ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(false));
    ARROW_RETURN_NOT_OK(scanner_builder->FragmentScanOptions(fragment_scan_options));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
    std::cout << "Table: " << table->ToString() << std::endl;
    return 0;
}
