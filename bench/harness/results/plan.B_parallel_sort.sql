;plan detail
;trace on
select /*+ PARALLEL(8) */
       o_orderpriority,
       o_orderstatus,
       count(*) as cnt,
       sum(o_totalprice) as revenue
from orders
where o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-07-01'
group by o_orderpriority, o_orderstatus
order by revenue desc, o_orderpriority, o_orderstatus;
;trace off
