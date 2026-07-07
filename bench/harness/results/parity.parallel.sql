;plan detail
;trace on
-- wmmid mid-size hash join (#144 P3/(B)): self LEFT OUTER JOIN mirroring
-- wmloc_outer_join_parity but over the 2.4M-row wmmid_t, so the build tier is
-- ~150MiB: spills under the 64MiB cap, promotes IN_MEM under P3 D1-②.
SELECT /*+ USE_HASH(a,b) PARALLEL(8) */ MOD(a.id,997) g, COUNT(*) c, SUM(CAST(a.id AS NUMERIC(38,0))) s, MIN(a.id) mn, MAX(a.id) mx
FROM wmmid_t a LEFT OUTER JOIN wmmid_t b ON a.id = b.id + 1
GROUP BY MOD(a.id,997);

SELECT /*+ USE_HASH(a,b) PARALLEL(8) */ MOD(a.id,997) g, COUNT(*) c, SUM(CAST(a.id AS NUMERIC(38,0))) s, MIN(a.id) mn, MAX(a.id) mx
FROM wmmid_t a RIGHT OUTER JOIN wmmid_t b ON a.id = b.id + 1
GROUP BY MOD(a.id,997);
