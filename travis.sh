#!/usr/bin/env sh
set -evx
env | sort

mkdir build || true
cd build
cmake -DCMAKE_CXX_FLAGS=$CXX_FLAGS ..
make
#make test