define pk_owner='BNA'
define pk_table='DEM_DEMARCHE_USAGER'

select 
   owner
  ,table_name
  ,constraint_name
  ,'alter table ' || owner || '.' || table_name || ' disable constraint '  || constraint_name || ';'
from 
  dba_constraints 
where 
  r_constraint_name in (select constraint_name
                        from   dba_constraints 
                        where  owner='&pk_owner'
                        and    table_name = '&pk_table'
                        and    constraint_type in ('P','U'));
