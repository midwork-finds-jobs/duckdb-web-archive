# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(web_archive
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
# DuckDB >= 2.0 builds but no longer links loaded extensions; link it so unittest can `require web_archive`
if(COMMAND duckdb_extension_statically_link)
  duckdb_extension_statically_link(web_archive)
endif()
