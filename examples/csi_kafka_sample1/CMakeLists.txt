project(csi_kafka_sample1)

SET(SRCS
csi_kafka_sample1.cpp
)

add_executable(csi_kafka_sample1 ${SRCS})
target_link_libraries(csi_kafka_sample1 csi_kafka ${EXT_LIBS})
