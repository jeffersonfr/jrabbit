include(FetchContent)

# fmt
FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
)
FetchContent_MakeAvailable(fmt)

# RabbitMq
# dependencies:
#  - sudo apt install librabbitmq-dev
set(JWT_BUILD_EXAMPLES OFF)
FetchContent_Declare(
        RabbitMq
        GIT_REPOSITORY https://github.com/alanxz/rabbitmq-c.git
        GIT_TAG master
        GIT_SUBMODULES
)
FetchContent_MakeAvailable(RabbitMq)

# expected
FetchContent_Declare(
        expected
        GIT_REPOSITORY https://github.com/TartanLlama/expected.git
)
FetchContent_MakeAvailable(expected)

FetchContent_GetProperties(expected)
if (expected_POPULATED)
        message("SOURCE_DIR: ${expected_SOURCE_DIR}")
        message("BINARY_DIR: ${expected_BINARY_DIR}")
endif ()

# create a target to use in src/
add_library(thirdparty INTERFACE)

target_link_libraries(thirdparty
        INTERFACE
                fmt
                rabbitmq-static
                expected
)

