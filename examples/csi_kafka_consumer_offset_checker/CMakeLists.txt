project(csi_kafka_consumer_offset_checker)

SET(SRCS
csi_kafka_consumer_offset_checker.cpp
)

add_executable(csi_kafka_consumer_offset_checker ${SRCS})
target_link_libraries(csi_kafka_consumer_offset_checker ${EXT_LIBS})
