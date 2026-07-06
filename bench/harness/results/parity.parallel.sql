;plan detail
;trace on
SELECT /*+ USE_HASH(a,b) PARALLEL(8) */ MOD(a.id,997) g, COUNT(*) c, SUM(CAST(a.id AS NUMERIC(38,0))) s, MIN(a.id) mn, MAX(a.id) mx
FROM wmloc_t a LEFT OUTER JOIN wmloc_t b ON a.id = b.id + 1
GROUP BY MOD(a.id,997);

SELECT /*+ USE_HASH(a,b) PARALLEL(8) */ MOD(a.id,997) g, COUNT(*) c, SUM(CAST(a.id AS NUMERIC(38,0))) s, MIN(a.id) mn, MAX(a.id) mx
FROM wmloc_t a RIGHT OUTER JOIN wmloc_t b ON a.id = b.id + 1
GROUP BY MOD(a.id,997);
