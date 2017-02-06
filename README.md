csi-kafka
=========
[![Build Status](https://travis-ci.org/bitbouncer/csi-kafka.svg?branch=master)](https://travis-ci.org/bitbouncer/csi-kafka)

A C++11 asyncronous producer/consumer library for Apache Kafka based on boost asio, supporting 0.10.1.0+ of the Kafka protocol, including commit/offset/fetch API. Does not require zookeeper integration

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
bash rebuild_linux.sh
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
bash rebuild_linux.sh
make
```

 
## Windows x64

Install build tools
```
CMake, Visual Studio 14
```
Build
```
mkdir source && cd source

set VISUALSTUDIO_VERSION_MAJOR=14
call "C:\Program Files (x86)\Microsoft Visual Studio %VISUALSTUDIO_VERSION_MAJOR%.0\VC\vcvarsall.bat" amd64


wget --no-check-certificate http://downloads.sourceforge.net/project/boost/boost/1.62.0/boost_1_62_0.zip
unzip boost_1_62_0.zip

git clone https://github.com/madler/zlib.git

cd zlib
mkdir build & cd build
cmake -G "Visual Studio 14 Win64" ..
msbuild zlib.sln
msbuild zlib.sln /p:Configuration=Release
cd ../..

cd boost_1_62_0
call bootstrap.bat
.\b2.exe -toolset=msvc-%VisualStudioVersion% variant=release,debug link=static address-model=64 architecture=x86 --stagedir=stage\lib\x64 stage -s ZLIB_SOURCE=%CD%\..\zlib headers log_setup log date_time timer thread system program_options filesystem regex chrono
cd ..

git clone https://github.com/bitbouncer/csi-async.git
git clone https://github.com/bitbouncer/csi-kafka.git

cd csi-kafka
call rebuild_win64_vc14.bat
```

