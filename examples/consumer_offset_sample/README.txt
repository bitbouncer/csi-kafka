#wget https://github.com/outbrain/zookeepercli/releases/download/v1.0.5/zookeepercli_1.0.5_amd64.deb
#sudo dpkg -i zookeepercli_1.0.5_amd64.deb

#create topic
bin/kafka-create-topic.sh --zookeeper localhost --replica 1 --partition 2 --topic test-text

#required for v0 API ??
zookeepercli -servers localhost /consumers/consumer_offset_sample/offsets "path_placeholder"

