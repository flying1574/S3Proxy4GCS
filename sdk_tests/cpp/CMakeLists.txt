cmake_minimum_required(VERSION 3.14)
project(s3proxy-sdk-test-cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# AWS SDK C++ 1.11.765
FetchContent_Declare(
    aws-sdk-cpp
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG 1.11.765
    GIT_SHALLOW TRUE
)
set(BUILD_ONLY "s3" CACHE STRING "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(aws-sdk-cpp)

# Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(s3proxy_cpp_tests
    s3proxy_test.cpp
)

target_link_libraries(s3proxy_cpp_tests
    PRIVATE
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(s3proxy_cpp_tests)
