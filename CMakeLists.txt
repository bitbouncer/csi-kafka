cmake_minimum_required(VERSION 2.8.1)
cmake_policy(SET CMP0015 NEW)

include_directories(${Boost_INCLUDE_DIRS} ${CMAKE_SOURCE_DIR})
	
add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-DBOOST_ASIO_HAS_STD_CHRONO)
add_definitions(-DBOOST_ALL_STATIC_LINK)

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
set(EXT_LIBS csi-kafka Ws2_32) 
endif()

if(__LINUX__)
link_directories(${Boost_LIBRARY_DIRS})
set(BOOST_LIBS ${BOOST_PATH}/stage/lib/libboost_log_setup.a ${BOOST_PATH}/stage/lib/libboost_log.a boost_date_time boost_timer boost_thread boost_system boost_program_options boost_filesystem boost_regex boost_chrono boost_iostreams)
set(EXT_LIBS csi-kafka ${BOOST_LIBS} rt c)
endif()

add_subdirectory(csi_kafka)

if(__BUILD_EXAMPLES__)
add_subdirectory(examples)
endif()


