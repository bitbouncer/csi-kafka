#!/usr/bin/env sh
set -evx
env | sort

wget http://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz/download -Oboost_1_59_0.tar.gz
tar xf boost_1_59_0.tar.gz

cd boost_1_59_0
if [ ! -f "./b2" ]; then ./bootstrap.sh; fi
./b2 -j 4 link=dynamic headers log_setup log date_time timer thread system program_options filesystem regex chrono 
./b2 -j 4 link=dynamic --with-log --with-thread --with-timer --with-program_options --with-iostreams
cd ..

git clone https://github.com/bitbouncer/csi-async.git

mkdir build || true
cd build

cmake -D__LINUX__=1 -DCSI_INCLUDE_PATH=/home/travis/build/bitbouncer/csi-kafka/csi-async -DBoost_INCLUDE_DIRS=/home/travis/build/bitbouncer/csi-kafka/boost_1_59_0 -DBoost_LIBRARY_DIRS=/home/travis/build/bitbouncer/csi-kafka/boost_1_59_0/stage/lib -D__BUILD_EXAMPLES__=1 .. 
make
