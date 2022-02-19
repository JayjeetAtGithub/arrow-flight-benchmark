#!/bin/bash
set -ex

apt update 
apt install -y python3 \
               python3-pip \
               python3-venv \
               python3-numpy \
               libsnappy-dev \
               libutf8proc-dev \
               libre2-dev \
               libthrift-dev \
               default-jre \
               default-jdk \
               maven

# ln -s libre2.so libre2.so.9
