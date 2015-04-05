project(highlevel_consumer_sample)

SET(SRCS
highlevel_consumer_sample.cpp
)

add_executable(highlevel_consumer_sample ${SRCS})
target_link_libraries(highlevel_consumer_sample ${EXT_LIBS})
