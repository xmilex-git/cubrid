SELECT MOD(id,997) g, COUNT(*) c, SUM(CAST(id AS NUMERIC(38,0))) s, MIN(id) mn, MAX(id) mx
FROM (SELECT /*+ PARALLEL(8) */ id FROM wmloc_t WHERE MOD(grp,2) = 0
      UNION
      SELECT id FROM wmloc_t WHERE MOD(grp,3) = 0) t
GROUP BY MOD(id,997);

SELECT MOD(id,997) g, COUNT(*) c, SUM(CAST(id AS NUMERIC(38,0))) s, MIN(id) mn, MAX(id) mx
FROM (SELECT /*+ PARALLEL(8) */ id FROM wmloc_t WHERE MOD(grp,2) = 0
      UNION ALL
      SELECT id FROM wmloc_t WHERE MOD(grp,3) = 0) t
GROUP BY MOD(id,997);
