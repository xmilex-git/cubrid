select /*+ USE_HASH(o,l) PARALLEL(8) */
       l.l_returnflag,
       count(*) as cnt,
       sum(l.l_extendedprice) as revenue
from lineitem l
join orders o on o.o_orderkey = l.l_orderkey
where l.l_shipdate >= date '1995-01-01'
  and l.l_shipdate < date '1996-01-01'
  and l.l_orderkey between 1 and 500000
  and o.o_orderkey between 1 and 500000
group by l.l_returnflag
order by l.l_returnflag;
