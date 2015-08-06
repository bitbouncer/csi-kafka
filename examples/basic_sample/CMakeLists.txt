project(lowlevel_sample)

SET(SRCS
lowlevel_sample.cpp
)

add_executable(lowlevel_sample ${SRCS})
target_link_libraries(lowlevel_sample ${EXT_LIBS})
