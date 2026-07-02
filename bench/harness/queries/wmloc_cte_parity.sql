WITH grp_x AS (
  SELECT id, grp FROM wmloc_t WHERE MOD(grp,7) = 0
)
SELECT /*+ PARALLEL(8) */ MOD(x.id,997) g, COUNT(*) c, SUM(CAST(x.id AS NUMERIC(38,0))) s, MIN(x.id) mn, MAX(x.id) mx
FROM grp_x x JOIN grp_x y ON x.grp = y.grp
GROUP BY MOD(x.id,997);
