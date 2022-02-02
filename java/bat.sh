#!/bin/bash
set -ex

mvn clean install
mvn package
