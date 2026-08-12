include(FetchContent)

macro(module_test)
    set(id ${ARGV0}_test)
    add_executable(${id} ${id}.cpp)
    add_test(${id} ${id} COMMAND $<TARGET_FILE:${id}>)
    target_link_libraries(${id}
            PRIVATE
                jrabbit::core
                jrabbit::thirdparty
                gtest_main
    )
    target_include_directories(${id}
            PRIVATE
                ${PROJECT_SOURCE_DIR}/include
    )
    target_include_directories(${id}
            INTERFACE
                .
                ${CMAKE_BINARY_DIR}
    )
    unset(id)
endmacro()

enable_testing()

module_test(rabbit_unit)
