;trace on text
SELECT /*+ USE_HASH */ COUNT(*) FROM t a, t b WHERE a.a = b.a;
;trace off
;exit
