project(fixed_rate_producer)

SET(SRCS
fixed_rate_producer.cpp
)

add_executable(fixed_rate_producer ${SRCS})
target_link_libraries(fixed_rate_producer csi_kafka ${EXT_LIBS})
