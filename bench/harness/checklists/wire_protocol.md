# Client List Wire-Protocol Determination

Determination: client-fetch is VPID-hardwired and defaults to class-B materialize for R0. Synthetic segment-native
streaming through a synthetic-VPID translation layer is deferred.

Evidence on `c08453968`:

| Coordinate | Evidence |
|---|---|
| `src/query/list_file.c:2312` | `xqfile_get_list_file_page(thread_p, query_id, vol_id, page_id, page_buf_p, page_size_p)` accepts a real `VOLID`/`PAGEID`. |
| `src/query/list_file.c:2324` | Builds a `VPID` from `vol_id` and `page_id`. |
| `src/query/list_file.c:2376` | Fetches the page with `qmgr_get_old_page(thread_p, &vpid, tfile_vfid_p)`. |
| `src/query/list_file.c:2384`-`:2388` | Follows overflow/next VPID links to append pages into the network buffer. |
| `src/query/list_file.c:2412` | Copies materialized page bytes into `page_buf_p`. |
| `src/communication/network_interface_sr.cpp:5052`-`:5054` | Server RPC unpacks `query_id`, `volid`, `pageid`. |
| `src/communication/network_interface_sr.cpp:5061` | Server RPC calls `xqfile_get_list_file_page` with those values. |
| `src/communication/network_interface_sr.cpp:5073`-`:5077` | Reply packs `page_size` and sends copied page bytes. |
| `src/communication/network_interface_cl.c:6737`-`:6744` | Client packs `query_id`, `volid`, `pageid` for `NET_SERVER_LS_GET_LIST_FILE_PAGE`. |
| `src/query/cursor.c:576` | Cursor fetch calls `qfile_get_list_file_page(cursor_id_p->query_id, vpid_p->volid, vpid_p->pageid, ...)`. |

R0 consequence: any client cursor, holdable cursor, or cache-published list whose consumer can fetch pages by VPID must
be materialized as a normal list file. Segment-native raw-fd streaming would require either a new wire shape or a
translation layer that maps synthetic VPID requests back to raw-fd segment coordinates. That work is explicitly deferred
from R0/P1a.
