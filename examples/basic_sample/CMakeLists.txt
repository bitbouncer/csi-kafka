project(basic_sample)

SET(SRCS
basic_sample.cpp
)

add_executable(basic_sample ${SRCS})
target_link_libraries(basic_sample ${EXT_LIBS})
