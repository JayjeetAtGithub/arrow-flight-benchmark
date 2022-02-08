// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// This example showcases various ways to work with Datasets. It's
// intended to be paired with the documentation.
//
// Taken from https://github.com/apache/arrow/blob/master/cpp/examples/arrow/dataset_documentation_example.cc

#include <arrow/api.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/iterator.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <vector>

namespace ds = arrow::dataset;
namespace fs = arrow::fs;
namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);


std::shared_ptr<arrow::Table> ScanWholeDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  // Create a dataset by scanning the filesystem for files
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                    ds::FileSystemFactoryOptions())
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Print out the fragments
  for (const auto& fragment : dataset->GetFragments().ValueOrDie()) {
    std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
  }
  // Read the entire dataset as a Table
  auto scan_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}

int main(int argc, char** argv) {
  if (argc < 3) {
    return EXIT_SUCCESS;
  }

  std::string uri = argv[1];
  std::string format_name = argv[2];
  std::string root_path;
  auto fs = fs::FileSystemFromUri(uri, &root_path).ValueOrDie();

  std::shared_ptr<ds::FileFormat> format = std::make_shared<ds::ParquetFileFormat>();
  std::string base_path = "/mnt/cephfs/dataset";

  std::shared_ptr<arrow::Table> table = ScanWholeDataset(fs, format, base_path);
  
  std::cout << "Read " << table->num_rows() << " rows" << std::endl;
  std::cout << table->ToString() << std::endl;
  return EXIT_SUCCESS;
}
