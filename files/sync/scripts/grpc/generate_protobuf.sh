#!/bin/bash

pip install virtualenv
virtualenv  /tmp/sync-core2/test_venv
source /tmp/sync-core2/test_venv/bin/activate
#for production
pip install grpcio-tools==1.15.0 grpcio==1.15.0

#required grpcio-tools pip module, version 1.15.0 (maybe later support too);for install: pin install grpcio-tools
python -m grpc_tools.protoc --python_out=../../bin/syncGrpc --grpc_python_out=../../bin/syncGrpc --proto_path=./ load_sale.proto
echo "Success!"
