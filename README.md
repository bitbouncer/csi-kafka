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



 
