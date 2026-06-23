select level as lvl
from db_root
connect by level <= 5000;
