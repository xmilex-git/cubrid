SELECT /*+ USE_MERGE(a,b) PARALLEL(8) */ MOD(a.id,997) g, COUNT(*) c, SUM(CAST(a.id AS NUMERIC(38,0))) s, MIN(a.id) mn, MAX(a.id) mx
FROM wmloc_t a JOIN wmloc_t b ON a.id = b.id
GROUP BY MOD(a.id,997);
