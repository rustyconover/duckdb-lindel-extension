cmake_minimum_required(VERSION 3.5)

set(CORROSION_VERBOSE_OUTPUT ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED 1)

include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG master
)
# Set any global configuration variables such as `Rust_TOOLCHAIN` before this line!
FetchContent_MakeAvailable(Corrosion)

# Import targets defined in a package or workspace manifest `Cargo.toml` file
corrosion_import_crate(MANIFEST_PATH "${CMAKE_SOURCE_DIR}/../duckdb_lindel_rust/Cargo.toml"
CRATES "duckdb_lindel_rust"
)

# Set extension name here
set(TARGET_NAME lindel)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

set(EXTENSION_SOURCES src/lindel_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

get_target_property(fake_includes duckdb_lindel_rust INCLUDE_DIRECTORIES)

target_link_libraries(${EXTENSION_NAME} duckdb_lindel_rust-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_lindel_rust)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

