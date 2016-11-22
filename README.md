csi-kafka
=========
[![Build Status](https://travis-ci.org/bitbouncer/csi-kafka.svg?branch=master)](https://travis-ci.org/bitbouncer/csi-kafka)

A C++11 asyncronous producer/consumer library for Apache Kafka based on boost asio, supporting v0.8.2+ of the Kafka protocol, including commit/offset/fetch API. Does not require zookeeper integration

- high and low level producers and consumers
- support for avro encoded topics (value or key/value)
- high level producers 
  - commit-callback on checkpoints, ie when all sub partitions have committed all messages up to a specific point.
  - ordered messages even in case of retries and changes to partition leaders 
- sync verisons of all (most?) calls


Missing
- compression
- timeouts on high level producers and consumers, Currently they keep on trying forever - handling a moving partition etc






Platforms: Windows / Linux

Building
## Ubuntu 14 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libcurl4-openssl-dev libbz2-dev libcurl3

```
Build
```
mkdir source && cd source
wget http://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz/download -Oboost_1_59_0.tar.gz
tar xf boost_1_59_0.tar.gz

cd boost_1_59_0
./bootstrap.sh
./b2 -j 8
cd ..

git clone https://github.com/bitbouncer/csi-async.git
git clone https://github.com/bitbouncer/csi-kafka.git

cd csi-kafka
mkdir build && cd build
cmake -D__LINUX__=1 -DCSI_INCLUDE_PATH=../csi-async -DBoost_INCLUDE_DIRS=../boost_1_59_0 -DBoost_LIBRARY_DIRS=$(pwd)/../../boost_1_59_0/stage/lib -D__BUILD_EXAMPLES__=1 .. 
make
```

## Ubuntu 16 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential g++ libboost-all-dev python-dev autotools-dev openssl libssl-dev libbz2-dev 

```
Build
```
mkdir source && cd source

git clone https://github.com/bitbouncer/csi-async.git
git clone https://github.com/bitbouncer/csi-kafka.git

cd csi-kafka
mkdir build && cd build
cmake -D__LINUX__=1 -DCSI_INCLUDE_PATH=../csi-async -D__BUILD_EXAMPLES__=1 .. 
make
```

 
