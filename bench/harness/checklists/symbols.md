# Per-Symbol Re-Resolution Checklist

Base: `c08453968` (`feature/temp-workmem-redesign` R0 base). The original plan coordinates were from
`69d78cac4`; all coordinates below were re-resolved on this tree.

| Symbol / site | c08453968 coordinate | Drift note |
|---|---:|---|
| `qexec_hash_gby_put_next` declaration | `src/query/query_executor.c:477` | Re-resolved; implementation below. |
| `qexec_hash_gby_put_next` implementation | `src/query/query_executor.c:4904` | Reads original tuple pages through `qmgr_get_old_page` at `:4927`. |
| GROUP BY part-list sort/merge-seal call | `src/query/query_executor.c:5576` | `sort_listfile(... qexec_hash_gby_put_next ...)`; teardown follows at `:5600`-`:5603`. |
| GROUP BY part-list teardown | `src/query/query_executor.c:5600` | Closes, destroys, frees `part_list_id`. |
| GROUP BY aggregate context teardown | `src/query/query_executor.c:27518` | Destroys `part_list_id`; `sorted_part_list_id` at `:27526`. |
| `merge_list_ids` | `src/query/parallel/px_scan/px_scan_result_handler.cpp:511` | Parallel result handler local helper. |
| `merge_list_ids` temp connect | `src/query/parallel/px_scan/px_scan_result_handler.cpp:526` | Connects worker list into temp merged list. |
| `merge_list_ids` dest connect | `src/query/parallel/px_scan/px_scan_result_handler.cpp:545` | Connects temp merged list into destination. |
| `merge_list_ids` writer result use | `src/query/parallel/px_scan/px_scan_result_handler.cpp:584` | Merges writer results. |
| `merge_list_ids` hash-groupby result use | `src/query/parallel/px_scan/px_scan_result_handler.cpp:589` | Merges `hgby_results` into `part_list_id`. |
| `qfile_connect_list` | `src/query/list_file.c:3130` | List concatenation primitive. |
| `qmgr_get_old_page` declaration | `src/query/query_manager.h:164` | Main temp-file page fetch. |
| `qmgr_get_old_page` implementation | `src/query/query_manager.c:2516` | Real VPID-based accessor. |
| `qfile_destroy_list` | `src/query/list_file.c:2269` | Retires temp VFID at `:2282`, clears list at `:2292`. |
| `qfile_clear_list_id` | `src/query/list_file.c:585` | Clears dependent list recursively at `:607`. |
| `qfile_free_list_id` | `src/query/list_file.c:623` | Calls `qfile_clear_list_id` then frees. |
| `file_temp_retire` declaration | `src/storage/file_manager.h:200` | Public temp retire API. |
| `file_temp_retire` implementation | `src/storage/file_manager.c:4447` | Calls `file_temp_retire_internal`. |
| `file_temp_retire_internal` | `src/storage/file_manager.c:4491` | Cache/destroy temp file implementation. |
| `qfile_set_dirty_page` declaration | `src/query/list_file.c:215` | Static helper. |
| `qfile_set_dirty_page` implementation | `src/query/list_file.c:1453` | Dirty/free list-page helper. |
| `qfile_append_list` | `src/query/list_file.c:2953` | Appends physical list pages. |
| `pgbuf_is_temporary_volume` declaration | `src/storage/page_buffer.c:1025` | Static inline. |
| `pgbuf_is_temporary_volume` implementation | `src/storage/page_buffer.c:5396` | Temp-volume discriminator used by TDE/page IO. |
| `QFILE_NOT_USE_MEMBUF` | `src/query/query_list.h:527` | `0x0800` list-file flag. |
| static `temp_mem_buffer_pages` cache | `src/query/query_manager.c:2958` | Initialized from `PRM_ID_TEMP_MEM_BUFFER_PAGES`. |
| `XASL_TO_BE_CACHED` define | `src/query/xasl.h:504` | Runtime cache flag. |
| `XASL_TO_BE_CACHED` px runtime gate, list input | `src/query/parallel/px_scan/px_scan.cpp:436` | Re-resolved on `c08453968`; disables this parallel scan path when `topn_items` or `XASL_TO_BE_CACHED` is set. |
| `XASL_TO_BE_CACHED` px runtime gate, index input | `src/query/parallel/px_scan/px_scan.cpp:866` | Re-resolved on `c08453968`; same runtime cache/top-N exclusion in the index scan path. |
| `XASL_TO_BE_CACHED` px runtime gate, heap input | `src/query/parallel/px_scan/px_scan.cpp:1322` | Re-resolved on `c08453968`; same runtime cache/top-N exclusion in the heap scan path. |
| `tde_encrypt_data_page` declaration | `src/storage/tde.h:190` | TDE data page encrypt entry. |
| `tde_encrypt_data_page` implementation | `src/storage/tde.c:909` | Copies reserved/watermark and writes nonce at `:943`. |
| `FILEIO_PAGE_RESERVED` / `prv.tde_nonce` | `src/storage/file_io.h:164` | `tde_nonce` field at `:175`. |
| `FILEIO_PAGE` framing | `src/storage/file_io.h:184` | Reserved header at `:188`, user page at `:189`, watermark at `:192`. |
| Buffer TDE flush path | `src/storage/page_buffer.c:10613` | Calls `tde_encrypt_data_page` before write. |

Open item for later phases: the old plan's symbol phrase `GROUP BY part_list merge-seal call` maps to the
`sort_listfile`/part-list close-destroy-free sequence in `query_executor.c`; no function with that exact name exists on
`c08453968`.
