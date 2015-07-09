#Create a garbage collected topic with replication

bin/kafka-topics --zookeeper zk_host:port --create --topic _uuid_schema --partitions 1 --replication-factor 3 --config cleanup.policy=compact  --config segment.bytes=1048576


# Register a schema 
curl -X POST -i --data "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}" http://localhost:8081/subjects
HTTP/1.1 200 OK
Content-Length: 47
Content-Type: text/html

{"uuid":"095d71cf-1255-6b9d-5e33-0ad575b3df5d"}


# Get a schema 
curl -i http://localhost:8081/subjects/095d71cf-1255-6b9d-5e33-0ad575b3df5d
HTTP/1.1 200 OK
Content-Length: 23
Content-Type: text/html

{"schema":"\"string\""}


