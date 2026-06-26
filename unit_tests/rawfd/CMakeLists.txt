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

set (TEST_RAWFD_SOURCES
  test_rawfd_registry.cpp
  )

SET_SOURCE_FILES_PROPERTIES(
  ${TEST_RAWFD_SOURCES}
  PROPERTIES LANGUAGE CXX
  )

if(TARGET cubrid)
  target_compile_definitions(cubrid PRIVATE
    RAWFD_UNIT_TEST
    )
endif()

add_executable(test_rawfd_registry
  ${TEST_RAWFD_SOURCES}
  )

set_target_properties(test_rawfd_registry PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )

target_compile_definitions(test_rawfd_registry PRIVATE
  SERVER_MODE
  RAWFD_UNIT_TEST
  ${COMMON_DEFS}
  )

target_include_directories(test_rawfd_registry PRIVATE
  ${TEST_INCLUDES}
  )

target_link_libraries(test_rawfd_registry LINK_PRIVATE
  test_common
  )
if(UNIX)
  target_link_libraries(test_rawfd_registry LINK_PRIVATE
    cubrid
    )
elseif(WIN32)
  target_link_libraries(test_rawfd_registry LINK_PRIVATE
    cubrid-win-lib
    )
else()
  message( SEND_ERROR "Rawfd unit testing is for unix/windows")
endif ()

enable_testing()
add_test(NAME rawfd_registry COMMAND $<TARGET_FILE:test_rawfd_registry>)
