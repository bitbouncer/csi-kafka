project(schema-registry)

SET(SRCS
schema-registry.cpp
)

add_executable(schema-registry ${SRCS})
target_link_libraries(schema-registry csi-http csi-http-parser ${EXT_LIBS})
