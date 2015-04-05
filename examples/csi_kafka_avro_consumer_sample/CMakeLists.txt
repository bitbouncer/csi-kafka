project(csi_kafka_avro_consumer_sample)

SET(SRCS
csi_kafka_avro_consumer_sample.cpp
)

add_executable(csi_kafka_avro_consumer_sample ${SRCS})
target_link_libraries(csi_kafka_avro_consumer_sample ${EXT_LIBS})
