;plan detail
;trace on
select /*+ USE_HASH(o1,o2) PARALLEL(8) */
       o1.o_orderpriority,
       count(*) as probe_hits,
       sum(o2.o_totalprice) as probe_revenue
from orders o1
join orders o2 on o2.o_custkey = o1.o_custkey
where o1.o_orderkey between 1 and 200000
  and o2.o_orderkey between 1 and 500000
group by o1.o_orderpriority
order by probe_hits desc, o1.o_orderpriority;
;trace off
