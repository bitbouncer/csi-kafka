#!/usr/bin/bash
rm -rf bin
rm -rf lib
rm -rf linux
mkdir linux
cd linux
cmake -D__LINUX__=1 ..
make
cd ..

