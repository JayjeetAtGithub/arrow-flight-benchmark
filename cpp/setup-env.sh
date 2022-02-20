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
               default-jre \
               maven

if [ ! -d "arrow" ]; then
    git clone https://github.com/apache/arrow
fi

cd arrow
git submodule update --init --recursive
git pull

mkdir -p cpp/release
cd cpp/release
git checkout apache-arrow-6.0.1

cmake -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_PYTHON=ON \
  -DARROW_ORC=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_DATASET=ON \
  -DARROW_CSV=ON \
  -DARROW_JAVA=ON \
  -DARROW_JNI=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_FLIGHT=ON \
  -DARROW_BUILD_EXAMPLES=ON \
  -DPARQUET_BUILD_EXAMPLES=ON \
  ..

make -j$(nproc) install
