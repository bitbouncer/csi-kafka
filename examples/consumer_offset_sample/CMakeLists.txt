project(consumer_offset_sample)

SET(SRCS
consumer_offset_sample.cpp
)

add_executable(consumer_offset_sample ${SRCS})
target_link_libraries(consumer_offset_sample csi_kafka ${EXT_LIBS})
