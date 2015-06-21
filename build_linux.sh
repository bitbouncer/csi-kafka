#!/usr/bin/bash
rm -rf bin
rm -rf lib
rm -rf linux
mkdir linux
cd linux
cmake -DCMAKE_BUILD_TYPE=Release -D__LINUX__=1 ..
make
cd ..

