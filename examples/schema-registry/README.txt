#Create a garbage collected topic with replication

bin/kafka-topics --zookeeper zk_host:port --create --topic _uuid_schema --partitions 1 --replication-factor 3 --config cleanup.policy=compact  --config segment.bytes=1048576

# Register a schema 
{
  "schema": "{\"type\": \"string\"}"
}    


curl -X POST -i --data "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}" http://localhost:8081/subjects
HTTP/1.1 200 OK
Content-Length: 47
Content-Type: text/html

{"uuid":"ccda95e1-5cdf-5438-be1b-c8cab417fede"}


curl -i http://localhost:8081/subjects/ccda95e1-5cdf-5438-be1b-c8cab417fede
HTTP/1.1 200 OK
Content-Length: 23
Content-Type: text/html

{"schema":"\"string\""}




curl -i http://localhost:8081/subjects/ccda95e1-4cdf-5438-be1b-c8cab417fede
HTTP/1.1 404 Not Found
Content-Length: 0

