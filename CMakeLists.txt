cmake_minimum_required(VERSION 2.8)
# set(CMAKE_VERBOSE_MAKEFILE ON)

PROJECT(epq)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  cmake_policy(SET CMP0068 NEW)
  SET(CMAKE_MACOSX_RPATH ON)
  SET(CMAKE_INSTALL_RPATH "@loader_path")
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
  SET(PLATFORM_LINKER_FLAGS "-Wl,-dead_strip -s")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  SET(PLATFORM_LINKER_FLAGS "-Wl,-rpath,./ -s -Wl,--gc-sections -Wl,--as-needed -lpthread")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")

endif()

SET(CMAKE_C_FLAGS "-std=c11")

SET(CMAKE_CXX_FLAGS "-std=c++11 -O3 -D__FILENAME__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath \$<))\"'")

SET(TURF_DIR "${CMAKE_SOURCE_DIR}/deps/turf")
SET(JUNCTION_DIR "${CMAKE_SOURCE_DIR}/deps/junction")
SET(JUNCTION_TURF_INCLUDES "${JUNCTION_DIR}" "${JUNCTION_DIR}/build/include" "${TURF_DIR}" "${JUNCTION_DIR}/build/turf/include")

SET(EPQ_INCLUDES "inc")
FILE(GLOB_RECURSE epq_src "src/*.cpp" "deps/junction/*.o")

ADD_LIBRARY(epq STATIC ${epq_src})
TARGET_INCLUDE_DIRECTORIES(epq PUBLIC ${EPQ_INCLUDES} PRIVATE "priv" ${JUNCTION_TURF_INCLUDES})

ADD_EXECUTABLE(epq_test "test/main.cpp")
TARGET_INCLUDE_DIRECTORIES(epq_test PUBLIC ${EPQ_INCLUDES})
TARGET_LINK_LIBRARIES(epq_test epq ${PLATFORM_LINKER_FLAGS})
