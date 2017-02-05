cmake_minimum_required(VERSION 2.8.1)
cmake_policy(SET CMP0015 NEW)


#if running this cmake selfcontained...
if (NOT DEFINED Boost_INCLUDE_DIRS)
SET(Boost_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/../boost)
endif()

if (NOT DEFINED Boost_LIBRARY_DIRS)
SET(Boost_LIBRARY_DIRS ${CMAKE_SOURCE_DIR}/../boost/stage/lib)
endif()

if (NOT DEFINED CSI_INCLUDE_PATH)
SET(CSI_INCLUDE_PATH ${CMAKE_SOURCE_DIR}/../csi-async)
endif()

include_directories(${CMAKE_SOURCE_DIR} ${CSI_INCLUDE_PATH} ${Boost_INCLUDE_DIRS})

if(WIN32)
add_definitions(-DMARCH_x86_64) 
add_definitions(-D_WIN32_WINNT=0x0602) # win8.1 to get nthll 64 bit
add_definitions(-DWINVER=0x0601)
add_definitions(-DCF_WINDOWS)
add_definitions(/MP)
add_definitions(/bigobj)
add_definitions(-D_UNICODE)
add_definitions(-DUNICODE)
add_definitions(-D_CRT_SECURE_NO_WARNINGS)
add_definitions(-D_SCL_SECURE_NO_WARNINGS)
add_definitions(-DSECURITY_WIN32)
add_definitions(/wd4996) #'strdup': The POSIX name for this item is deprecated
add_definitions(/wd4197) # 'cf_atomic32' : top-level volatile in cast is ignored
add_definitions(/wd4200) #nonstandard extension used : zero-sized array in struct/union 
link_directories(${Boost_LIBRARY_DIRS}/$(Platform)/lib)

SET(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib/$(Platform))
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin/$(Platform))
#linux
else()
add_definitions(-D__LINUX__)
add_definitions(-std=c++11)
add_definitions(-DBOOST_LOG_DYN_LINK)
link_directories(${Boost_LIBRARY_DIRS})
if (NOT DEFINED CSI_BOOST_LIBS)
set(CSI_BOOST_LIBS boost_log_setup boost_log boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono boost_iostreams pthread rt c)
endif()
set(LIBRARY_OUTPUT_PATH     ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH  ${CMAKE_SOURCE_DIR}/bin)
endif()

add_subdirectory(csi_kafka)

if(__BUILD_EXAMPLES__)
add_subdirectory(examples)
endif()
