project(async)

SET(SRCS
async.cpp
)

add_executable(async ${SRCS})
target_link_libraries(async csi_kafka ${EXT_LIBS})
