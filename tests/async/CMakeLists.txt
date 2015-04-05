project(async)

SET(SRCS
async.cpp
)

add_executable(async ${SRCS})
target_link_libraries(async ${EXT_LIBS})
