
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/test)

find_path(GTEST_HEADER NAMES gtest/gtest.h)
find_library(GTEST_LIB NAMES gtest)
find_library(GTEST_MAIN_LIB NAMES gtest_main)

file(GLOB TEST_PIDB_SRCS "test_*.cpp")

foreach(PIDB_UT ${TEST_PIDB_SRCS})
        get_filename_component(PIDB_UT_WE ${PIDB_UT} NAME_WE)
        add_executable(${PIDB_UT_WE} ${PIDB_UT})
        target_link_libraries(${PIDB_UT_WE}
                           ${GTEST_MAIN_LIB}
                          ${GTEST_LIB}
                            pidb
                            )
endforeach()

