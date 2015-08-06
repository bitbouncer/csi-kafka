#wget https://github.com/outbrain/zookeepercli/releases/download/v1.0.5/zookeepercli_1.0.5_amd64.deb
#sudo dpkg -i zookeepercli_1.0.5_amd64.deb

#create topic
kafka-topics --zookeeper zk1 --create --replication-factor 1 --partitions 2 --topic test-text

#required for v0 API?
zookeepercli -servers zk1 -c creater /consumers/consumer_offset_sample/offsets "path_placeholder"

#required for v1 API
zookeepercli -servers zk1 -c creater /consumers/consumer_offset_sample/offsets "path_placeholder"
zookeepercli -servers zk1 -c creater /consumers/consumer_offset_sample/owners "path_placeholder"
