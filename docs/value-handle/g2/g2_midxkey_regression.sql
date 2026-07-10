-- G2 MIDXKEY preservation regression (handoff §8.2) — tpch_sf10, READ-ONLY.
-- Purpose: prove the feature branch did not reroute the composite-index (MIDXKEY)
-- fast path (element-wise index_cmpdisk) through anything slower. Compare
-- develop merge-base vs post-cleanup feature release: identical plan, identical
-- results, elapsed delta; confirmed regression >2% on any cell is a P3/ADOPT blocker.
-- Composite indexes used: lineitem PK (l_orderkey, l_linenumber),
--                         partsupp PK (ps_partkey, ps_suppkey),
--                         lineitem FK (l_partkey, l_suppkey).
-- NOTE: index BUILD/REBUILD timing cannot run read-only on tpch_sf10; it is
-- measured separately on a scratch DB (see g2-report.md).

-- M1: composite-index RANGE scan on first key column (btree MIDXKEY comparisons dominate)
SELECT /*+ RECOMPILE */ COUNT(*), MIN(l_linenumber), MAX(l_linenumber)
FROM lineitem
WHERE l_orderkey BETWEEN 1000000 AND 3000000;

-- M2: composite-index EQUALITY on full key (point lookups over a driven range)
SELECT /*+ RECOMPILE */ COUNT(*), SUM(CAST(ps_availqty AS BIGINT))
FROM partsupp
WHERE ps_partkey BETWEEN 100000 AND 400000 AND ps_suppkey BETWEEN 1 AND 50000;

-- M3: ORDER BY following composite index order (sort must be absorbed by the index;
--     range widened so elapsed is above timer resolution)
SELECT /*+ RECOMPILE */ SUM(CAST(l_orderkey AS BIGINT)), SUM(l_linenumber), COUNT(*)
FROM (
  SELECT l_orderkey, l_linenumber
  FROM lineitem
  WHERE l_orderkey BETWEEN 2000000 AND 6000000
  ORDER BY l_orderkey, l_linenumber
  LIMIT 500000
) x;

-- M4: index skip scan — predicate on the SECOND key column only, forced onto the
--     composite PK (a single-column FK index on ps_suppkey exists and would otherwise win)
SELECT /*+ RECOMPILE INDEX_SS */ COUNT(*), SUM(CAST(ps_availqty AS BIGINT))
FROM partsupp
WHERE ps_suppkey BETWEEN 700 AND 800
USING INDEX pk_partsupp_ps_partkey_ps_suppkey;

-- M5: composite FK index range (second composite index, wider rows).
--     Range sized so the touched heap pages fit the 512M data buffer: warm runs are
--     CPU-bound (comparator + heap lookup), not OS-IO-bound — an IO-bound variant
--     (20001 partkeys, ~579K ioreads/run) proved unable to resolve a 2% question
--     under co-tenant load (see g2-report.md).
SELECT /*+ RECOMPILE */ COUNT(*), SUM(l_quantity)
FROM lineitem
WHERE l_partkey BETWEEN 500000 AND 500500;
