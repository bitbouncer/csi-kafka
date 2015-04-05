project(csi_kafka_avro_producer_sample)

SET(SRCS
csi_kafka_avro_producer_sample.cpp
)

add_executable(csi_kafka_avro_producer_sample ${SRCS})
target_link_libraries(csi_kafka_avro_producer_sample ${EXT_LIBS})
