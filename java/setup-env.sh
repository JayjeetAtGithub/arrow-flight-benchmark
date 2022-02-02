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

if [ ! -d "arrow" ]; then
    git clone https://github.com/apache/arrow
    cd arrow/
    git submodule update --init --recursive
fi

cd arrow
git pull

mkdir -p cpp/release
cd cpp/release


cmake -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_PYTHON=ON \
  -DARROW_ORC=ON \
  -DARROW_JAVA=ON \
  -DARROW_JNI=ON \
  -DARROW_DATASET=ON \
  -DARROW_CSV=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZSTD=ON \
  ..

make -j4 install

cd ../../

mkdir -p arrow/java/dist
cp -r arrow/cpp/release/release/libarrow_dataset_jni.so* arrow/java/dist

mvn="mvn -B -DskipTests -Dcheckstyle.skip -Drat.skip=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
mvn="${mvn} -T 2C"
cd arrow/java
${mvn} clean install package -P arrow-jni -pl dataset,format,memory,vector -am -Darrow.cpp.build.dir=arrow/cpp/release/release
