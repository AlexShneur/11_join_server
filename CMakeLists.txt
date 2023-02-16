cmake_minimum_required(VERSION 3.12)

set(PATCH_VERSION "1" CACHE INTERNAL "Patch version")
set(PROJECT_VERSION 0.0.${PATCH_VERSION})

project(join_server VERSION ${PROJECT_VERSION})

find_package(Boost COMPONENTS system REQUIRED)

add_executable(join_server main.cpp sqlite3)

set_target_properties(join_server PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

set_target_properties(join_server PROPERTIES
        COMPILE_DEFINITIONS BOOST_TEST_DYN_LINK
        INCLUDE_DIRECTORIES ${Boost_INCLUDE_DIR}
)

target_include_directories(join_server
    PRIVATE "${CMAKE_BINARY_DIR}"
)

target_link_libraries(join_server
	${Boost_LIBRARIES}
)

install(TARGETS join_server RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(CPACK_PACKAGE_CONTACT alexsh93@live.ru)
include(CPack)		
