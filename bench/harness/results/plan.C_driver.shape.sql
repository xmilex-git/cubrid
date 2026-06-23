set optimization level 257;
;plan simple
select /*+ NO_PARALLEL_SCAN */
       o.o_orderpriority,
       count(*) as order_count,
       sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
from orders o
join lineitem l on l.l_orderkey = o.o_orderkey
where o.o_orderdate >= date '1994-01-01'
  and o.o_orderdate < date '1994-02-01'
  and o.o_orderkey between 1 and 500000
  and l.l_orderkey between 1 and 500000
group by o.o_orderpriority
order by revenue desc, o.o_orderpriority;
