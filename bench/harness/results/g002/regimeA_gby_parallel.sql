;trace on text
SELECT COUNT(*) FROM (SELECT a+0 AS k FROM t GROUP BY a+0) z;
;trace off
;exit
