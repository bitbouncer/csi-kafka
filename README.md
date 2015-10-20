csi-kafka
=========

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
see
https://github.com/bitbouncer/csi-build-scripts


If you only wan't kafka on ubuntu14 you can also do (probably broken since I've updated the dependencies to boost 1.59)

## Ubuntu 14 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libcurl4-openssl-dev libbz2-dev libcurl3 libboost-all-dev libpq-dev

```
Build
```
#until boost 1.58? when this is supposed to be included
git clone https://github.com/boostorg/endian.git
#back some things out of trunk to compile under boost 1.54 
sed -i "s:<boost/predef/detail/endian_compat.h>:<boost/detail/endian.hpp>:" endian/include/boost/endian/arithmetic.hpp
sed -i "s:<boost/predef/detail/endian_compat.h>:<boost/detail/endian.hpp>:" endian/include/boost/endian/conversion.hpp
sed -i "s:<boost/predef/detail/endian_compat.h>:<boost/detail/endian.hpp>:" endian/include/boost/endian/buffers.hpp

sed -i "s:<boost/core/scoped_enum.hpp>:<boost/detail/scoped_enum_emulation.hpp>:" endian/include/boost/endian/arithmetic.hpp
sed -i "s:<boost/core/scoped_enum.hpp>:<boost/detail/scoped_enum_emulation.hpp>:" endian/include/boost/endian/conversion.hpp
sed -i "s:<boost/core/scoped_enum.hpp>:<boost/detail/scoped_enum_emulation.hpp>:" endian/include/boost/endian/buffers.hpp


git clone https://github.com/bitbouncer/csi-build-scripts.git
git clone https://github.com/bitbouncer/csi-kafka.git

cd csi-kafka
bash -e build_linux.sh
cd ..
```

 
