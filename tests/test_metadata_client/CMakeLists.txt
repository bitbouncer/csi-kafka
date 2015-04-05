project(test_metadata_client)

SET(SRCS
test_metadata_client.cpp
)

add_executable(test_metadata_client ${SRCS})
target_link_libraries(test_metadata_client ${EXT_LIBS})
