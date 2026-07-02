-- wmloc fixture (redesign #78/#92): a small, fast-to-load table sized to
-- reliably trigger BOTH real parallel-worker engagement (parity.sh's
-- passthrough-tautology guard needs >=2 actual workers on the heap scan --
-- empirically ~300K rows is too small on this build/optimizer, 5.12M works)
-- and NEW(Tapeset) backing conversion (qfile_list_make_new_backed() converts
-- any closed SORT/DISTINCT output regardless of size when its gate is on, so
-- row count is not required for that half -- it just needs to be nonzero).
--
-- Usage: DB_NAME=wmloc csql -u dba wmloc -i wmloc_fixture_setup.sql
-- Runtime: ~3-4 min on a debug build (doubling inserts, not row-by-row).
DROP TABLE IF EXISTS wmloc_t;
CREATE TABLE wmloc_t (id INT, grp INT, val VARCHAR(64));
INSERT INTO wmloc_t (id, grp, val)
SELECT LEVEL, MOD(LEVEL, 2000), CAST(MOD(LEVEL, 2000) AS VARCHAR(64))
FROM db_root
CONNECT BY LEVEL <= 5000;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 5000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 10000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 20000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 40000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 80000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 160000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 320000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 640000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 1280000, grp, val FROM wmloc_t;
COMMIT;
INSERT INTO wmloc_t (id, grp, val) SELECT id + 2560000, grp, val FROM wmloc_t;
COMMIT;
