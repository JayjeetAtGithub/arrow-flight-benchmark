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


arrow::Result<std::shared_ptr<arrow::Table>> Scan() {
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
    return table;
}

int main(int argc, char** argv) {
    // time start
    auto start = std::chrono::high_resolution_clock::now();
    // scan
    auto table = Scan().ValueOrDie();
    std::cout << table->num_rows() << std::endl;
    // time end
    auto end = std::chrono::high_resolution_clock::now();
    // print time
    std::cout << "time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    return 0;
}
