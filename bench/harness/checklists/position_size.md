# POSITION-SIZE Memory Checklist

Base: `c08453968`. Current sizes are derived from the structure fields and from in-tree comments where available.
The hash-list scan code explicitly documents `QFILE_TUPLE_SIMPLE_POS` as 12 bytes at
`src/query/scan_manager.c:9243`-`:9245`. `QFILE_TUPLE_POSITION` is 40 bytes on the target 64-bit ABI:
two enum/int fields (8), `VPID` with padding (8), offset (4), tuple pointer (8), tplno (4), plus tail padding.

Projected coordinate for P1a/P1b: fixed-size discriminated coordinate with either real `VPID` or
`{raw_fd_segment_id, page_index, tuple_offset}`. The projected full position is budgeted at 48 bytes; projected
simple position is budgeted at 24 bytes. The DB_BIT parent-position encoding remains `sizeof(struct) * 8`, so stored
tuple layout stays fixed-size even after widening.

| Storage site | Coordinate | Current bytes / entry | Projected bytes / entry | Extra bytes | Accountant note |
|---|---:|---:|---:|---:|---|
| `src/query/scan_manager.h:445` `SCAN_POS::ls_tplpos` | full | 40 | 48 | +8 | One saved scan position per merge/correlated scan state. |
| `src/query/query_executor.c:347` `PARENT_POS_INFO::tpl_pos` | full | 40 | 48 | +8 | CONNECT BY parent stack entry; budget by max hierarchy depth. |
| `src/query/query_executor.c:17366` DB_BIT parent-pos in tuple | full, sizeof-derived bits | 40 bytes payload | 48 bytes payload | +8 | Must remain fixed-size; type width changes only when struct widens. |
| `src/query/query_executor.c:18920` recalc parent-pos DB_BIT | full, sizeof-derived bits | 40 bytes payload | 48 bytes payload | +8 | Same stored-tuple rule. |
| `src/query/query_executor.c:18951` recalc child parent-pos DB_BIT | full, sizeof-derived bits | 40 bytes payload | 48 bytes payload | +8 | Same stored-tuple rule. |
| `src/query/query_executor.c:18986` recalc after stack pop DB_BIT | full, sizeof-derived bits | 40 bytes payload | 48 bytes payload | +8 | Same stored-tuple rule. |
| `src/query/query_hash_scan.h:86` `HASH_SCAN_VALUE::pos` | simple | 12 | 24 | +12 | One entry per hybrid hash-list scan tuple. |
| `src/query/query_hash_scan.c:682` allocation of simple pos | simple | 12 | 24 | +12 | `db_private_alloc(sizeof(QFILE_TUPLE_SIMPLE_POS))`. |
| `src/query/query_hash_join.c:1319` HJ split estimate | simple | 12 | 24 | +12 | Update CEIL_PTVDIV input. |
| `src/query/query_hash_join.c:2644` HJ method threshold | simple | 12 | 24 | +12 | Update in-memory/hybrid decision. |
| `src/query/query_hash_join.c:2650` HJ tuple budget | simple | 12 | 24 | +12 | Update division denominator. |
| `src/query/query_hash_join.c:2669` HJ tuple budget | simple | 12 | 24 | +12 | Update division denominator. |
| `src/query/scan_manager.c:9241` hash-list scan method threshold | simple | 12 | 24 | +12 | Update documented 44 bytes/row to 56 bytes/row. |

Worst-case P1a accountant delta:

- Hybrid hash-list/HJ entry: `+12 * tuple_cnt` bytes where the entry stores `QFILE_TUPLE_SIMPLE_POS`.
- CONNECT BY parent stack: `+8 * max_connect_by_depth` bytes.
- DB_BIT parent-position payload in materialized tuples: `+8 * connect_by_tuple_cnt` bytes if `QFILE_TUPLE_POSITION`
  is widened and encoded by `sizeof(new_position) * 8`.
- Generic saved scan positions: `+8 * saved_scan_position_count`.

The dominant term is the stored DB_BIT parent-position payload for CONNECT BY and the simple-position hash entries for
hash-list scans/HJ. P1a must charge both to the work-memory accountant before allowing raw-fd positioned storage.
