# Single-Worker Raw-FD Read-Accessor Parity Definition

Purpose: define the P1b gate before product code exists. This R0 document does not implement the raw-fd accessor.

## Test Name

`rawfd_single_worker_tde_positioned_read_parity`

## Inputs

- One worker thread.
- Temporary raw-fd file opened through the P1b raw-fd file abstraction.
- Page count: at least 257 pages to cross simple power-of-two/cache assumptions.
- Page size: `DB_PAGESIZE` payload inside full `FILEIO_PAGE` framing.
- TDE mode: enabled and using the same temp-page path as `tde_encrypt_data_page`.
- Deterministic page payload: page index, tuple offset markers, and checksum bytes repeated across the payload.

## Write Path Under Test

1. Allocate one full `FILEIO_PAGE` buffer.
2. Fill `FILEIO_PAGE_RESERVED` and payload exactly as the normal temp-page path expects.
3. Encrypt through `tde_encrypt_data_page(plain, algo, true, cipher)`.
4. Write the full encrypted `FILEIO_PAGE` to raw-fd segment `{segment_id, page_index}`.
5. Record the widened coordinate `{raw_fd_segment_id, page_index, tuple_offset}` for multiple tuple offsets per page.

## Read Path Under Test

1. For each recorded coordinate, call the positioned raw-fd read accessor.
2. The accessor must read full `FILEIO_PAGE` framing, decrypt through the normal TDE decrypt path, and return the
   requested page/tuple bytes.
3. Repeat reads in non-sequential order to prove it is not accidentally using append-order state.

## Assertion

- Full decrypted `FILEIO_PAGE` bytes match the original plain page bytes byte-for-byte.
- For every recorded tuple offset, returned tuple bytes match the original payload slice byte-for-byte.
- `FILEIO_PAGE_RESERVED::tde_nonce` is preserved through encrypt/decrypt semantics; see `src/storage/file_io.h:175` and
  `src/storage/tde.c:943`/`:992`.
- No buffer-pool `VPID` lookup is used on the raw-fd branch.

## P1b Gate Integration

Add this as a focused unit/integration test beside the raw-fd accessor implementation. It gates P1b before enabling any
class A-prime consumer. A failure blocks class A-prime positioned consumers and forces class-B materialization fallback.
