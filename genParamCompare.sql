define P1="&1"
define P2="&2"
define P3="&3"
define P4="&4"
define P5="&5"

set feedback off
set serveroutput on
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    genParamCompare.sql [mode] 
|   
|      Generates a SQL SCript to by run on another DATABASE to compare init parameters :
|
|   Parameters :
|       mode     : ALL=All parameters except those known to contain names)
|                  MOD= ONly non default parameters                  
|       
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/
-- -----------------------------------------------------------------
-- Parameters (use P1 -- PN, to ease script test in SQL*Dev)
-- -----------------------------------------------------------------
--
--  Comparison mode
--
define mode="case when '&P1' is null then 'MOD' else upper('&P1') end"
-
prompt set lines 500 trimout on trimspool off pages 2000 tab off
prompt column name format a50
prompt column value format a50
set lines 500 heading off feedback off pagesize 2000
column ord format 999 noprint
column a format a500


select 
  10 ord , '      select null INST_ID ,null NAME,null VALUE,null VALUE_' || name || ' from dual where 1=2' a
from v$database
union
select
  20 ord , 'UNION select inst_id,name,value,'''|| value || ''' from gv$parameter where name = ''' || name || ''' and inst_id=' || inst_id || ' and nvl(value,''$$@@$$'') != nvl('''||value||''',''$$@@$$'') '  a
from 
  gv$parameter 
where 
      (&mode='ALL' or isdefault='FALSE')
  and name  not like 'log_archive%' 
  and name not in ('local_listener','remote_listener'
,'audit_file_dest'
,'background_dump_dest'
,'cluster_interconnects'
,'control_files'
,'core_dump_dest'
,'db_name'
,'db_unique_name'
,'dg_broker_config_file1'
,'dg_broker_config_file2'
,'dispatchers'
,'instance_name'
,'service_names'
,'spfile'
,'user_dump_dest'
,'audit_file_dest'
,'background_dump_dest')
union
select 
  30 ord , 'order by 2,1;'
from dual
/
