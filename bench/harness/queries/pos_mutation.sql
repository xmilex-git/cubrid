select level as lvl,
       connect_by_isleaf as is_leaf,
       connect_by_iscycle as is_cycle
from db_root
connect by nocycle level <= 5000;
