set lines 200 pages 10000 tab off
col owner format a15
col table_name format a35
col column_name format a35
col data_default format a100
with 
function get_data_default(u in varchar2,t in varchar2,c in varchar2) return varchar2 as
  tmp varchar2(32767) ;
begin
  select data_default
  into   tmp
  from   dba_tab_columns
  where  owner = u
  and    table_name = t
  and    column_name = c ;
  return (tmp) ;
end ;
select 
  *
from (
      select
         owner
        ,table_name
        ,column_name
        ,get_data_default(owner,table_name,column_name) data_default
      from
        dba_tab_columns
      where 
        data_default is not null
)
where
      1=1 -- Add your restrictions here
--      upper(data_default) like '%TO_DATE%'
--  and upper(data_default) not like '%YY%'
--  and upper(data_default) not like '%HH%'
/  
 
