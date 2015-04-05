project(csi_kafka_sample2)

SET(SRCS
csi_kafka_sample2.cpp
)

add_executable(csi_kafka_sample2 ${SRCS})
target_link_libraries(csi_kafka_sample2 ${EXT_LIBS})
