project(offset_commit_sample)

SET(SRCS
offset_commit_sample.cpp
)

add_executable(offset_commit_sample ${SRCS})
target_link_libraries(offset_commit_sample ${EXT_LIBS})
