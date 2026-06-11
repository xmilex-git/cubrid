#
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
#

project(test_stream_channel)

set(SRC
    ${PARALLEL_QUERY_DIR}/px_stream_channel.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_channel.cpp
    src/test_stream_channel.cpp
)

add_executable(test_stream_channel ${SRC})

# self-contained: px_stream_channel needs only px_interrupt.hpp (PARALLEL_QUERY_DIR) and
# error_context.hpp/memory_wrapper.hpp (BASE_DIR); without SERVER_MODE the memory wrapper
# is inert, so no engine library link is required.
target_include_directories(test_stream_channel PRIVATE
    ${PARALLEL_QUERY_DIR}
    ${BASE_DIR}
)

find_package(Threads REQUIRED)
target_link_libraries(test_stream_channel PRIVATE Threads::Threads)
