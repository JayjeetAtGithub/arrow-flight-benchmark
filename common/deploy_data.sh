#!/bin/bash
set -ex

mkdir -p /mnt/data/flight_dataset
wget https://skyhook-ucsc.s3.us-west-1.amazonaws.com/16MB.uncompressed.parquet
for i in {1..100}; do
  cp 16MB.uncompressed.parquet /mnt/data/flight_dataset/16MB.uncompressed.parquet.$i
done
