cmake_minimum_required(VERSION 2.8)
INCLUDE(${CMAKE_SOURCE_DIR}/../build-scripts/CMakeDependencies.txt)

if(WIN32)
set(EXT_LIBS csi-kafka libeay32 ssleay32 Ws2_32) 
endif()

if(__LINUX__)
set(EXT_LIBS csi-kafka ${BOOST_PATH}/stage/lib/libboost_log_setup.a ${BOOST_PATH}/stage/lib/libboost_log.a boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono curl ssl crypto rt pthread c)
endif()

add_subdirectory(csi_kafka)

if(__BUILD_EXAMPLES__)
add_subdirectory(examples)
endif()


