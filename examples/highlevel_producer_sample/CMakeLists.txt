project(highlevel_producer_sample)

SET(SRCS
highlevel_producer_sample.cpp
)

add_executable(highlevel_producer_sample ${SRCS})
target_link_libraries(highlevel_producer_sample csi-kafka ${CSI_BOOST_LIBS})
