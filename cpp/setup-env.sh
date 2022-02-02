#!/bin/bash
set -ex

apt update 
apt install -y python3 \
               python3-pip \
               python3-venv \
               python3-numpy \
               cmake \
               libradospp-dev \
               rados-objclass-dev \
               llvm \
               default-jdk \
               maven

git clone https://github.com/apache/arrow
cd arrow/
git submodule update --init --recursive

mkdir -p cpp/release
cp cpp/release

cmake -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_BUILD_EXAMPLES=ON \
  -DPARQUET_BUILD_EXAMPLES=ON \
  -DARROW_PYTHON=ON \
  -DARROW_ORC=ON \
  -DARROW_DATASET=ON \
  -DARROW_CSV=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZSTD=ON \
  ..

make -j4 install
