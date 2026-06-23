#!/usr/bin/env bash
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

set -euo pipefail

cat <<'MSG'
rawfd_single_worker_tde_positioned_read_parity entry point:
  temp_page_store::rawfd_single_worker_tde_positioned_read_parity(THREAD_ENTRY *)

The entry writes 257 full FILEIO_PAGE-framed raw-fd pages through tde_encrypt_data_page(..., is_temp=true),
then issues non-sequential positioned reads through rawfd_pos_read and byte-compares the decrypted payloads.
It constructs the raw-fd file directly, so the product write guard may remain false for the P1b safety gate.

Wire this function into the focused server/unit runner in the build tree and execute it with TDE loaded.
MSG
