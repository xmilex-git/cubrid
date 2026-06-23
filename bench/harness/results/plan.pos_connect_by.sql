;plan detail
;trace on
select level as lvl
from db_root
connect by level <= 5000;
;trace off
