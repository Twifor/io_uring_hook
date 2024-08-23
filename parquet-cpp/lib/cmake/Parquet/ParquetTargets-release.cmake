#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Parquet::parquet_shared" for configuration "RELEASE"
set_property(TARGET Parquet::parquet_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Parquet::parquet_shared PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "thrift::thrift"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libparquet.so.1300.0.0"
  IMPORTED_SONAME_RELEASE "libparquet.so.1300"
  )

list(APPEND _IMPORT_CHECK_TARGETS Parquet::parquet_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_Parquet::parquet_shared "${_IMPORT_PREFIX}/lib/libparquet.so.1300.0.0" )

# Import target "Parquet::parquet_static" for configuration "RELEASE"
set_property(TARGET Parquet::parquet_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Parquet::parquet_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libparquet.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS Parquet::parquet_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_Parquet::parquet_static "${_IMPORT_PREFIX}/lib/libparquet.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
