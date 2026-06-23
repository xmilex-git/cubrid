# Class-A / A-prime / B Consumer-Callsite Checklist

Base: `c08453968`. Classification terms:

- Class A: sequential re-emit; segment-native streaming can preserve sequential access.
- Class A-prime: positioned/random access; raw-fd redesign must provide positioned access or materialize.
- Class B: externally visible/client/cache cursor paths; default to materialize in R0 determination.

## Scan Save / Jump API Users

| Coordinate | Site | Class | Reason |
|---|---|---|---|
| `src/query/scan_manager.h:445` | `SCAN_POS::ls_tplpos` stores `QFILE_TUPLE_POSITION` | A-prime | Saved list-file scan position. |
| `src/query/scan_manager.c:7979` | `scan_save_scan_pos` | A-prime | Saves list-file scan position. |
| `src/query/scan_manager.c:7983` | calls `qfile_save_current_scan_tuple_position` | A-prime | Stores real VPID coordinate. |
| `src/query/scan_manager.c:7996` | `scan_jump_scan_pos` | A-prime | Restores saved position. |
| `src/query/scan_manager.c:8013` | calls `qfile_jump_scan_tuple_position` | A-prime | Random jump back into list file. |
| `src/query/query_executor.c:7022` | merge outer join saves inner scan | A-prime | Merge-join inner rewind. |
| `src/query/query_executor.c:7149` | merge outer join jumps inner scan | A-prime | Rewinds inner group. |
| `src/query/query_executor.c:7211` | merge outer join jumps inner scan again | A-prime | Additional inner-group rewind. |

## QFILE_TUPLE_POSITION Encoder / Decoder / Save / Restore / Jump

| Coordinate | Site | Class | Reason |
|---|---|---|---|
| `src/query/query_list.h:486` | `QFILE_TUPLE_POSITION` typedef | A-prime | Real VPID coordinate definition. |
| `src/query/list_file.h:218` | save API declaration | A-prime | API stores scan position. |
| `src/query/list_file.h:219` | jump API declaration | A-prime | API restores scan position. |
| `src/query/list_file.c:4986` | `qfile_save_current_scan_tuple_position` | A-prime | Copies status/position/VPID/offset/tpl/tplno. |
| `src/query/list_file.c:5060` | `qfile_jump_scan_tuple_position` | A-prime | Fetches target page by VPID and positions tuple. |
| `src/query/list_file.c:5074` | read-only jump page fetch | A-prime | `qmgr_get_old_page_read_only`. |
| `src/query/list_file.c:5079` | writable jump page fetch | A-prime | `qmgr_get_old_page`. |
| `src/query/list_file.c:6586` | `qfile_add_tuple_get_pos_in_list` | A-prime | Encodes position of newly inserted tuple. |
| `src/query/list_file.c:6613` | fills `QFILE_TUPLE_POSITION` | A-prime | Stores offset/tplno/VPID. |
| `src/query/query_executor.c:6461` | merge join saves `inner_tplpos` | A-prime | Merge-join inner rewind. |
| `src/query/query_executor.c:6561` | merge join jumps to `inner_tplpos` | A-prime | Merge-join random jump. |
| `src/query/list_file.c:2612` | `qfile_advance_group` saves next position | A-prime | DISTINCT/group advance boundary. |
| `src/query/list_file.c:2613` | `qfile_advance_group` jumps last scan | A-prime | DISTINCT/group positioned access. |
| `src/query/query_hash_join.c:3944` | local `QFILE_TUPLE_POSITION` in HJ probe | A-prime | Hash-join probe/rewind. |
| `src/query/query_hash_join.c:4002` | `MAKE_TUPLE_POSTION` from simple pos | A-prime | Hybrid HJ build-side position decode. |
| `src/query/query_hash_join.c:4003` | jump to hybrid HJ tuple | A-prime | Random probe. |
| `src/query/query_hash_join.c:4032` | `MAKE_TFTID_TO_TUPLE_POSTION` | A-prime | Hash-file HJ position decode. |
| `src/query/query_hash_join.c:4033` | jump to hash-file HJ tuple | A-prime | Random probe. |
| `src/query/scan_manager.c:9001` | hash-list scan local position | A-prime | Scan-manager hash-list scan. |
| `src/query/scan_manager.c:9043` | hybrid hash-list decode | A-prime | Converts simple pos to full tuple pos. |
| `src/query/scan_manager.c:9044` | hybrid hash-list jump | A-prime | Random jump. |
| `src/query/scan_manager.c:9063` | hash-file decode from `TFTID` | A-prime | File hash scan decode. |
| `src/query/scan_manager.c:9064` | hash-file jump | A-prime | Random jump. |
| `src/query/scan_manager.c:9107` | next hybrid hash-list simple pos | A-prime | Iterates hash entries. |
| `src/query/scan_manager.c:9110` | next hybrid jump | A-prime | Random jump. |
| `src/query/scan_manager.c:9127` | next hash-file decode | A-prime | File hash scan decode. |
| `src/query/scan_manager.c:9128` | next hash-file jump | A-prime | Random jump. |

## QFILE_TUPLE_SIMPLE_POS Storage / Decode

| Coordinate | Site | Class | Reason |
|---|---|---|---|
| `src/query/query_hash_scan.h:74` | `QFILE_TUPLE_SIMPLE_POS` typedef | A-prime | Compact VPID/offset coordinate. |
| `src/query/query_hash_scan.h:86` | `HASH_SCAN_VALUE::pos` | A-prime | Hash-list scan stores tuple coordinate. |
| `src/query/query_hash_scan.c:682` | allocate simple pos | A-prime | One per hybrid hash-list entry. |
| `src/query/query_hash_scan.c:690` | save offset/VPID | A-prime | Encodes temp-file coordinate. |
| `src/query/scan_manager.c:9241` | memory budget uses `sizeof(QFILE_TUPLE_SIMPLE_POS)` | A-prime | Accountant input for hybrid hash-list scan. |
| `src/query/query_hash_join.c:1319` | HJ split budget uses simple pos | A-prime | Hash join work memory estimate. |
| `src/query/query_hash_join.c:2644` | HJ method decision uses simple pos | A-prime | Hash join in-memory/hybrid threshold. |
| `src/query/query_hash_join.c:2650` | HJ tuple capacity division | A-prime | Accountant-derived tuple budget. |
| `src/query/query_hash_join.c:2669` | HJ tuple capacity division | A-prime | Accountant-derived tuple budget. |

## DB_BIT Parent-Position Encode / Decode Sites

| Coordinate | Site | Class | Reason |
|---|---|---|---|
| `src/query/query_executor.c:17231` | initializes parent position as 8-bit null | A-prime | CONNECT BY parent-position pseudo column. |
| `src/query/query_executor.c:17360` | adds parent tuple and gets position | A-prime | Encodes parent coordinate. |
| `src/query/query_executor.c:17366` | `db_make_bit(... sizeof(parent_pos) * 8)` | A-prime | Stored tuple layout is sizeof-derived. |
| `src/query/query_executor.c:18333` | `db_get_bit` parent position decode | A-prime | Cycle check follows parent chain. |
| `src/query/query_executor.c:18344` | jump to decoded parent position | A-prime | Random parent lookup. |
| `src/query/query_executor.c:18699` | parent-position domain `DB_TYPE_BIT` | A-prime | Fixed bit domain. |
| `src/query/query_executor.c:18920` | recalc parent pos encode | A-prime | `sizeof(pos_info_p->tpl_pos) * 8`. |
| `src/query/query_executor.c:18949` | recalc saves previous tuple position | A-prime | Stack parent coordinate. |
| `src/query/query_executor.c:18951` | recalc encode after level increase | A-prime | `sizeof(pos_info_p->tpl_pos) * 8`. |
| `src/query/query_executor.c:18986` | recalc encode after stack pop | A-prime | `sizeof(pos_info_p->tpl_pos) * 8`. |
| `src/query/query_opfunc.c:7255` | correlated/parent pos `db_get_bit` decode | A-prime | Prior/root style access. |
| `src/query/query_opfunc.c:7266` | jump to decoded tuple position | A-prime | Random lookup. |
| `src/query/query_opfunc.c:7376` | correlated/parent pos decode | A-prime | Parent coordinate access. |
| `src/query/query_opfunc.c:7387` | jump to decoded tuple position | A-prime | Random lookup. |
| `src/query/query_opfunc.c:7710` | correlated/parent pos decode | A-prime | Path/root access. |
| `src/query/query_opfunc.c:7721` | jump to decoded tuple position | A-prime | Random lookup. |

## Sequential Re-Emit / Materialization Consumers

| Coordinate | Site | Class | Reason |
|---|---|---|---|
| `src/query/parallel/px_scan/px_scan_result_handler.cpp:511` | `merge_list_ids` | A | Sequentially connects worker result lists. |
| `src/query/parallel/px_scan/px_scan_result_handler.cpp:526` | temp merged `qfile_connect_list` | A | Sequential list concatenation. |
| `src/query/parallel/px_scan/px_scan_result_handler.cpp:545` | destination `qfile_connect_list` | A | Sequential list concatenation. |
| `src/query/parallel/px_scan/px_scan_result_handler.cpp:1445` | aggregate list connect | A | Sequential aggregate result merge. |
| `src/query/list_file.c:2312` | `xqfile_get_list_file_page` | B | Client page fetch is VPID/pageid based. |
| `src/communication/network_interface_sr.cpp:5061` | server RPC calls `xqfile_get_list_file_page` | B | Wire request contains query/vol/page. |
| `src/communication/network_interface_cl.c:6723` | client `qfile_get_list_file_page` | B | Packs query id, volid, pageid. |
| `src/query/cursor.c:576` | client cursor fetch by VPID | B | Holdable/client cursor path. |
| `src/query/list_file.c:5620` | list cache entry count used for cache invalidation | B | Cache-publish/list-cache consumers must materialize. |
| `src/query/list_file.c:6013` | list cache invalidation after preserved temp retire | B | Cache lifetime tied to materialized list file. |
| `src/query/list_file.c:6767` | `qfile_set_tuple_column_value` | A-prime | Positional mutation of tuple in list page. |
