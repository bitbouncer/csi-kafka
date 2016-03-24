#!/usr/bin/bash
rm -rf bin
rm -rf lib
rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -D__LINUX__=1 -DBoost_INCLUDE_DIRS=../boost -DBoost_LIBRARY_DIRS=../boost/stage/lib -D__BUILD_EXAMPLES__=1 .. 
make -j4
cd ..

