#!/usr/bin/env sh
set -evx
env | sort

mkdir build || true
mkdir build/$GTEST_TARGET || true
cd build/$GTEST_TARGET
cmake -DCMAKE_CXX_FLAGS=$CXX_FLAGS \
      ../../$GTEST_TARGET
make
#make test