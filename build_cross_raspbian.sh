#!/usr/bin/bash
export PI_TOOLS_HOME=~/xtools/tools
rm -rf bin
rm -rf lib
rm -rf build
rm -f CMakeCache.txt
rm -f ./lib/armeabi-v7a/*

mkdir build
cd build
cmake -D__RASPBIAN__=1 -D__CSI_HAS_OPENSSL__=1 -D__LINUX__=1 -DCMAKE_TOOLCHAIN_FILE=../csi-build-scripts/toolchains/raspberry.ia32.cmake ..
make -j4
cd ..


