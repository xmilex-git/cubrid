#
#  Copyright 2008 Search Solution Corporation
#  Copyright 2016 CUBRID Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

project (test_value_handle_bench)

# cubrid_rel sha (see main.cpp's file header comment for the "same mechanism as release_string.c"
# rationale): release_string.c's own makestring()-wrapped macros (RELEASE_STRING/BUILD_NUMBER,
# cmake/version.h.cmake) never carry an actual git revision, only a version NUMBER — since the
# P2.1 CSV schema wants a "cubrid_rel sha", this harness follows the same CMake-time
# execute_process()-embeds-a-compile-definition mechanism but sources the value from
# `git describe --always --dirty` (LOW hygiene fix, architect finding: `--always` itself already
# supplies the "short-sha fallback" this value needs whenever no tag is reachable from HEAD, and
# `--dirty` flags an uncommitted worktree so a results CSV row can never be silently attributed to
# a clean commit it wasn't actually built from), with a documented fallback when git metadata is
# unavailable at all (e.g. a tarball checkout with no .git) — main.cpp then just prints
# VHB_CUBRID_REL_SHA verbatim into every results-CSV row.
set (VHB_CUBRID_REL_SHA "unknown")
find_package (Git QUIET)
if (GIT_FOUND)
  execute_process (
    COMMAND ${GIT_EXECUTABLE} describe --always --dirty
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE VHB_GIT_SHA_OUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE VHB_GIT_RC
    )
  if (VHB_GIT_RC EQUAL 0 AND VHB_GIT_SHA_OUT)
    set (VHB_CUBRID_REL_SHA ${VHB_GIT_SHA_OUT})
  endif ()
endif ()
message ("    value_handle_bench: VHB_CUBRID_REL_SHA = ${VHB_CUBRID_REL_SHA}")

set (TEST_VALUE_HANDLE_BENCH_SRC
  main.cpp
  fixture.cpp
  harness_util.cpp
  variant_cmpdisk.cpp
  variant_valueslot.cpp
  variant_pervalue.cpp
  variant_flatbuffers.cpp
  )
set (TEST_VALUE_HANDLE_BENCH_H
  value_handle_bench.hpp
  )
SET_SOURCE_FILES_PROPERTIES(
  ${TEST_VALUE_HANDLE_BENCH_SRC}
  PROPERTIES LANGUAGE CXX
  )

add_executable (test_value_handle_bench
  ${TEST_VALUE_HANDLE_BENCH_SRC}
  ${TEST_VALUE_HANDLE_BENCH_H}
  )

target_compile_definitions (test_value_handle_bench PRIVATE
  SERVER_MODE
  ${COMMON_DEFS}
  VHB_CUBRID_REL_SHA=\"${VHB_CUBRID_REL_SHA}\"
  )

# Peer-vendored flatbuffers headers (E3-Flatbuffers, bench-dir only — NEVER production CMake).
# Guarded so a missing/partial vendoring pass never breaks *configure*; variant_flatbuffers.cpp
# itself is responsible for compiling (and returning nullptr from make_variant_c_flatbuffers())
# without them if the vendoring effort failed — that guard lives in that file, not here.
set (VHB_FLATBUFFERS_INCLUDE "")
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/flatbuffers)
  set (VHB_FLATBUFFERS_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

target_include_directories (test_value_handle_bench PRIVATE
  ${TEST_INCLUDES}
  ${VHB_FLATBUFFERS_INCLUDE}
  )

if (UNIX)
  target_link_libraries (test_value_handle_bench PRIVATE
    cubrid
    )
elseif (WIN32)
  target_link_libraries (test_value_handle_bench PRIVATE
    cubrid-win-lib
    )
else ()
  message (SEND_ERROR "value_handle_bench unit testing is for unix/windows")
endif ()
