project(lowlevel_producer_sample)

SET(SRCS
lowlevel_producer_sample.cpp
)

add_executable(lowlevel_producer_sample ${SRCS})
target_link_libraries(lowlevel_producer_sample csi-kafka ${BOOST_LIBS})
