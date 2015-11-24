cmake_minimum_required(VERSION 2.8)
INCLUDE(${CMAKE_SOURCE_DIR}/../csi-build-scripts/CMakeDependencies.txt)

if(WIN32)
set(EXT_LIBS csi-kafka libcurl libeay32 ssleay32 Ws2_32) 
endif()

if(__LINUX__)
set(EXT_LIBS csi-kafka boost_log_setup boost_log boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono curl ssl crypto rt pthread c)
endif()


add_subdirectory(csi_kafka)

if(__BUILD_EXAMPLES__)
add_subdirectory(examples)
endif()


