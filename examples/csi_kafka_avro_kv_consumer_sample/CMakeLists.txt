project(csi_kafka_avro_kv_consumer_sample)

SET(SRCS
contact_info.h
contact_info_key.h
csi_kafka_avro_kv_consumer_sample.cpp
)

add_executable(csi_kafka_avro_kv_consumer_sample ${SRCS})
target_link_libraries(csi_kafka_avro_kv_consumer_sample csi_kafka ${EXT_LIBS})
