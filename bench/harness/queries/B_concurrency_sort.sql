select /*+ PARALLEL(8) */
       l_shipmode,
       l_returnflag,
       count(*) as cnt,
       sum(l_quantity) as qty,
       sum(l_extendedprice) as revenue
from lineitem
where l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1995-01-01'
  and l_orderkey between 1 and 500000
group by l_shipmode, l_returnflag
order by revenue desc, l_shipmode, l_returnflag;
