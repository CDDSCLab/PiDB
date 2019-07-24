cmake_minimum_required(VERSION  3.09)
project(pidb C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" ON)
option(LINK_TCMALLOC "Link tcmalloc if possible" ON)
option(BUILD_UNITTEST "Build Test" OFF)
# execute_process(
#    COMMAND bash -c "find /usr/local/ -type d -path \"*include/levledb\" | xargs dirname | xargs dirname | tr -d '\n'"
#    OUTPUT_VARIABLE OUTPUT_PATH
#)

set(CMAKE_PROJECT_VERSION 0)
set(CMAKE_PROJECT_VERSION_MAJOR 0)
set(CMAKE_PROJECT_VERSION_MAJOR 1)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

include(FindThreads)
include(FindProtobuf)

include_directories(
  "${PROJECT_SOURCE_DIR}"
)

set(PIDB_PUBLIC_INCLUDE_DIR "include/pidb")
set(PIDB_SERVER_DIR "src/server")
if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER "${PROJECT_SOURCE_DIR}/src/server/pidb.proto")
protobuf_generate_cpp(MASTER_PROTO_SRC MASTER_PROTO_HEADER "${PROJECT_SOURCE_DIR}/src/server/master.proto")
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(EXAMPLE_LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
    find_library(BRAFT_LIB NAMES braft)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
    find_library(BRAFT_LIB NAMES libbraft.a braft)
endif()

if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
    message (FATAL_ERROR "Fail to find braft")
endif()

include_directories(${BRAFT_INCLUDE_PATH})


find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)

if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

if (LINK_TCMALLOC)
    find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
    if (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
        set(CMAKE_CXX_FLAGS "-DBRPC_ENABLE_CPU_PROFILER")
        include_directories(${GPERFTOOLS_INCLUDE_DIR})
    else ()
        set (GPERFTOOLS_LIBRARIES "")
    endif ()
endif ()

set(CMAKE_CPP_FLAGS "-DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DNDEBUG -O2 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()



add_subdirectory("${PROJECT_SOURCE_DIR}/src/libs/json11")
include_directories(${PROTO_HEADER} ${MASTER_PROTO_HEADER})
include_directories("${PROJECT_SOURCE_DIR}/src/libs")
include_directories(${LEVELDB_INCLUDE_PATH})
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/include")
include_directories("${PROJECT_SOURCE_DIR}/src/server")


add_library(pidb SHARED "")
add_executable(pidb_server src/server/main.cpp)
target_sources(pidb PUBLIC
	${PROTO_SRC}
    ${PROTO_HEADER}
    ${MASTER_PROTO_SRC}
    ${MASTER_PROTO_HEADER}
    "${PROJECT_SOURCE_DIR}/src/server/route_table.cpp"
    "${PROJECT_SOURCE_DIR}/src/server/route_table.h"
    "${PROJECT_SOURCE_DIR}/src/port/port_stdcxx.h"
    "${PROJECT_SOURCE_DIR}/src/port/port.h"
    "${PROJECT_SOURCE_DIR}/src/include/pidb/status.h"
    "${PROJECT_SOURCE_DIR}/src/util/status.cc"
    "${PROJECT_SOURCE_DIR}/src/port/port_stdcxx.h"

    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/server.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/server.cpp"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/raftnode.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/raftnode.cpp"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/shareddb.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/pidb_service_impl.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/pidb_service_impl.cpp"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/master_service_impl.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/master_service_impl.cpp"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/context_cache.h"
    "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/context_cache.cpp"

    "${PROJECT_SOURCE_DIR}/src/include/pidb/options.h"
        "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/remote_file_send.h"
        "${PROJECT_SOURCE_DIR}/${PIDB_SERVER_DIR}/remote_file_send.cpp"


)

target_include_directories(pidb_server
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${GPERFTOOLS_LIBRARIES}
    ${LEVELDB_LIB}
    ${BRAFT_LIB}
    ${BRPC_LIB}
        json11
    rt
    ssl
    crypto
    dl
    z
    )

if (BUILD_UNITTEST)
    enable_testing()
    add_subdirectory(test)
endif ()


target_link_libraries(pidb
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")

target_link_libraries(pidb_server pidb dl)
