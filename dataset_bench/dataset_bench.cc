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
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri("file:///mnt/data/flight_dataset", &path)); 
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
    ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(false));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
    return table;
}

int main(int argc, char** argv) {
    for (int i = 0; i < 10; i++) {
        // time start
        auto start = std::chrono::high_resolution_clock::now();
        // scan
        auto table = Scan().ValueOrDie();
        // time end
        auto end = std::chrono::high_resolution_clock::now();
        // print time
        std::cout << (double)std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()/1000 << std::endl;
    }
    return 0;
}
