#!/bin/bash

export QTRADE_PY_UNITTEST_DATA_DIR=$(pwd)/unittest_resources

for package in q*
do
    echo $package
    export PYTHONPATH=$PYTHONPATH:$(pwd)/${package}/src/main/python
    pip install -r $package/requirements.txt
done
