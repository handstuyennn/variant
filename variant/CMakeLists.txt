cmake_minimum_required(VERSION 3.1)

project(VariantExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
include_directories(../duckdb/extension/json/include)
include_directories(../duckdb/extension/json/yyjson/include)

set(VARIANT_EXTENSION_FILES variant-extension.cpp variant.cpp vector_proxy.cpp
                             converters.cpp)

add_library(variant_extension STATIC ${VARIANT_EXTENSION_FILES})

install(
  TARGETS variant_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
