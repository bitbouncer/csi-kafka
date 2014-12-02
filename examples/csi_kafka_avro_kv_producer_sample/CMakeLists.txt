project(csi_kafka_avro_kv_producer_sample)

SET(SRCS
csi_kafka_avro_kv_producer_sample.cpp
contact_info.h
contact_info_key.h
)

add_executable(csi_kafka_avro_kv_producer_sample ${SRCS})
target_link_libraries(csi_kafka_avro_kv_producer_sample csi_kafka ${EXT_LIBS})
