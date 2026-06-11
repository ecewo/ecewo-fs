cmake_minimum_required(VERSION 3.14)
project(ecewo-fs VERSION 0.2.0 LANGUAGES C)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(PROJECT_IS_TOP_LEVEL)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

option(ECEWO_FS_BUILD_SHARED "Build ecewo-fs as a shared library" OFF)

if (PROJECT_IS_TOP_LEVEL AND NOT TARGET ecewo::ecewo)
  include(FetchContent)

  FetchContent_Declare(
    ecewo
    GIT_REPOSITORY https://github.com/savashn/ecewo.git
    GIT_TAG v4.0.0
    GIT_SHALLOW TRUE
  )

  FetchContent_MakeAvailable(ecewo)
endif()

if(ECEWO_FS_BUILD_SHARED)
  add_library(ecewo-fs SHARED ecewo-fs.c)
else()
  add_library(ecewo-fs STATIC ecewo-fs.c)
endif()

add_library(ecewo::fs ALIAS ecewo-fs)

include(GenerateExportHeader)
generate_export_header(ecewo-fs
  BASE_NAME ECEWO_FS
  EXPORT_FILE_NAME "${CMAKE_CURRENT_BINARY_DIR}/include/ecewo-fs-export.h"
)

target_include_directories(ecewo-fs PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

set_target_properties(ecewo-fs PROPERTIES
  OUTPUT_NAME ecewo-fs
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  EXPORT_NAME fs
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(ecewo-fs PUBLIC ecewo::ecewo)

if (PROJECT_IS_TOP_LEVEL AND TARGET ecewo::ecewo)
  ecewo_add(mock@dev)
  enable_testing()

  add_executable(ecewo_fs_test
    tests/test.c
  )

  target_include_directories(ecewo_fs_test
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/
      ${CMAKE_CURRENT_SOURCE_DIR}/tests
  )

  target_link_libraries(ecewo_fs_test
    PRIVATE
      ecewo::ecewo
      ecewo::fs
      ecewo::mock
  )

  if (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(ecewo_fs_test PRIVATE
      -Wall
      -Wextra
      -Werror
      -Wstrict-prototypes
      -Wincompatible-pointer-types
      -Wno-unused-parameter
    )
  endif()

  add_test(
    NAME ecewo_fs_all
    COMMAND ecewo_fs_test
  )
endif()
