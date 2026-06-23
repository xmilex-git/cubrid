set optimization level 257;
;plan simple
select /*+ LEADING(l o) USE_HASH(o) PARALLEL(8) */ l.l_returnflag, count(*) as cnt
from lineitem l join orders o on o.o_orderkey = l.l_orderkey
where l.l_shipdate >= date '1995-01-01' and l.l_shipdate < date '1995-02-01'
group by l.l_returnflag order by l.l_returnflag;
