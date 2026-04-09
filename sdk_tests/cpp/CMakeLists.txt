cmake_minimum_required(VERSION 3.14)
project(s3proxy-sdk-test-cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(googletest)

# Find AWS SDK (installed via apt or vcpkg)
find_package(AWSSDK REQUIRED COMPONENTS s3)

enable_testing()

add_executable(s3proxy_cpp_tests
    s3proxy_test.cpp
)

target_link_libraries(s3proxy_cpp_tests
    PRIVATE
    ${AWSSDK_LINK_LIBRARIES}
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(s3proxy_cpp_tests)
