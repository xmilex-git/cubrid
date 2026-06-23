select /*+ PARALLEL(8) */
       o_orderpriority,
       count(*) as order_count,
       sum(o_totalprice) as revenue
from orders
where o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1995-01-01'
group by o_orderpriority
order by revenue desc, o_orderpriority;
