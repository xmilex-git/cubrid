-- wmmid fixture (#144 P3/(B)): a MID-SIZE hash-join build whose in-memory
-- footprint lands in (64MiB, 256MiB] -- i.e. it SPILLS under the current 64MiB
-- work_mem accountant cap, but is promoted IN-MEMORY under P3 D1 escape-hatch ②
-- (cap = min(max(work_mem, data_buffer/8), data_buffer/2) = 256MiB @ 512MiB pool).
--
-- Sized at 2,400,000 rows: outer_join over 5.12M rows builds ~320MiB in-mem, so
-- ~2.4M rows -> ~150MiB build (mid-band).  Schema mirrors wmloc_t.
--
-- Usage: DB_NAME=wmloc csql -u dba wmloc -i wmmid_fixture_setup.sql
DROP TABLE IF EXISTS wmmid_t;
CREATE TABLE wmmid_t (id INT, grp INT, val VARCHAR(64));
INSERT INTO wmmid_t (id, grp, val)
SELECT id, grp, val FROM wmloc_t WHERE id <= 2400000;
COMMIT;
-- verify: SELECT COUNT(*) FROM wmmid_t;  -> 2400000
