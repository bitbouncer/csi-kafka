project(mirror_maker_sample)

SET(SRCS
mirror_maker_sample.cpp
)

add_executable(mirror_maker_sample ${SRCS})
target_link_libraries(mirror_maker_sample csi_kafka ${EXT_LIBS})
