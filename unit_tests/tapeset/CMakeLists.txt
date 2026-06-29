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

# Synthetic N-Tape split gate for the Phase1 1A scan contract (redesign G005,
# issue #70).  Drives qfile::tapeset_scan over in-memory Tapes (no server boot)
# to verify forward / reverse / jump / empty-skip / terminal / S_END-on-last.

set (TEST_TAPESET_SOURCES
  test_tapeset_scan.cpp
  )

SET_SOURCE_FILES_PROPERTIES(
  ${TEST_TAPESET_SOURCES}
  PROPERTIES LANGUAGE CXX
  )

add_executable(test_tapeset_scan
  ${TEST_TAPESET_SOURCES}
  )

set_target_properties(test_tapeset_scan PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )

target_compile_definitions(test_tapeset_scan PRIVATE
  SERVER_MODE
  ${COMMON_DEFS}
  )

target_include_directories(test_tapeset_scan PRIVATE
  ${TEST_INCLUDES}
  )

target_link_libraries(test_tapeset_scan LINK_PRIVATE
  test_common
  )
if(UNIX)
  target_link_libraries(test_tapeset_scan LINK_PRIVATE
    cubrid
    )
elseif(WIN32)
  target_link_libraries(test_tapeset_scan LINK_PRIVATE
    cubrid-win-lib
    )
else()
  message( SEND_ERROR "Tapeset unit testing is for unix/windows")
endif ()

enable_testing()
add_test(NAME tapeset_scan COMMAND $<TARGET_FILE:test_tapeset_scan>)
