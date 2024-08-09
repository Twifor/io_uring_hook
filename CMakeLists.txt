cmake_minimum_required(VERSION 3.10)

project(io_uring_hook VERSION 1.0)
set(hook uringhook)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
add_definitions(-DBTHREAD_USE_FAST_PTHREAD_MUTEX)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

set(URING_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/liburing/src/liburing.a)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/liburing/src/include)
add_subdirectory(brpc)

add_custom_target(build_liburing
    COMMAND ./configure
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/liburing
    COMMENT "Configuring liburing"
    COMMAND ${CMAKE_MAKE_PROGRAM} -j8
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/liburing
    DEPENDS ${CMAKE_SOURCE_DIR}/liburing/Makefile
    COMMENT "Building liburing"
)


add_library(uring_static STATIC IMPORTED)
set_property(TARGET uring_static APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(uring_static PROPERTIES IMPORTED_LOCATION_NOCONFIG "${URING_LIBRARY}")
add_dependencies(uring_static build_liburing)

add_library(${hook} SHARED hook.cpp)
target_include_directories(${hook} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/build/brpc/output/include)
target_link_libraries(${hook} dl uring_static)
target_link_libraries(${hook} brpc-shared protobuf pthread)
set_target_properties(${hook} PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_executable(test test.cpp)
target_include_directories(test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/build/brpc/output/include)
target_link_libraries(test brpc-shared protobuf pthread gflags ssl crypto dl leveldb snappy z glog)