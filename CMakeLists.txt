
cmake_minimum_required(VERSION 3.9)
project(tdchunk VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "-std=c++11 -pthread -g")


add_library(tdchunk "")
target_sources(tdchunk
  PRIVATE
    "db/bloom_filter.cc"
    "db/bloom_filter.h"
    "db/db_manager.cc"
    "db/db_manager.h"
    "db/db.cc"
    "db/db.h"
    "db/extraction.h"
    "db/file_helper.cc"
    "db/file_helper.h"
    "db/file_list.cc"
    "db/file_list.h"
    "util/coding.cc"
    "util/coding.h"
)

include_directories(
  "."
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
add_subdirectory(lib/pybind11)
pybind11_add_module(py_tdchunk db/bind.cc )
target_link_libraries(py_tdchunk PUBLIC tdchunk)

add_executable(testdb
  "db/test_db.cc"
)
target_link_libraries(testdb tdchunk)

