define P1="&1"
define P2="&2"
define P3="&3"
define P4="&4"
define P5="&5"

set feedback off
set serveroutput on
declare
 dummy number ;
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    genParamCompare.sql [mode] [PDB]
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
  begin
    select 1 into dummy FROM DUAL where SYS_CONTEXT('USERENV', 'CON_NAME') = 'CDB$ROOT';
  exception when no_data_found then
    raise_application_error(-20001,'This script must be run from the CDB$ROOT container');
  end ;
end ;
/
-- -----------------------------------------------------------------
-- Parameters (use P1 -- PN, to ease script test in SQL*Dev)
-- -----------------------------------------------------------------
--
--  Comparison mode
--

define mode="case when '&P1' is null then 'MOD' else upper('&P1') end"
define pdb="case when '&P2' is null then '' else upper('&P2') end"

prompt col 1 new_value 1
prompt select null "1" from dual where 1=2 ;;
prompt set define !
prompt set verify off
prompt whenever sqlerror exit failure
prompt declare
prompt  dummy number ;;
prompt begin
prompt   select 1 into dummy FROM DUAL where SYS_CONTEXT('USERENV', 'CON_NAME') = 'CDB$ROOT';;
prompt exception when no_data_found then
prompt   raise_application_error(-20001,'This script must be run from the CDB$ROOT container');;
prompt end ;;
prompt /
prompt set lines 500 trimout on trimspool on pages 2000 tab off
prompt column name format a40
prompt column value format a40
set lines 500 heading off feedback off pagesize 2000
column ord format 999 noprint
column a format a400 
column b format a400 newline
set recsep off

select 
   10 ord 
  , '      select null INST_ID ,null NAME,null VALUE,null VALUE_' || name || ' from dual where 1=2' a
  , 'UNION select null INST_ID ,null NAME,null VALUE,null VALUE_' || name || ' from dual where 1=2' b
--  ,'Name'
--  ,'Value@' || name
from v$database
UNION
select /* Get parameters at PDB level */
   20 ord 
  ,'UNION select 9,name,value$,'''|| f.value$ || ' (' ||p.name||')'|| ''' from pdb_spfile$ where name = ''' || f.name ||  ''' and nvl(value$,''$$@@$$'') != nvl('''||f.value$||''',''$$@@$$'') and pdb_uid = (select con_uid from v$pdbs where name=nvl(upper(''!1''),''@X@'')) '  a
  ,'UNION select 9,'''||f.name||''',''  *** Not Set in PDB ***'','''|| f.value$ || ' (' ||p.name||')' || ''' from dual where not exists (select 1 from pdb_spfile$  where name = ''' || f.name || ''' and pdb_uid = (select con_uid from v$pdbs where name=nvl(upper(''!1''),''@X@'')))'  a
--  ,f.name
--  ,f.value$ || ' (' || p.name||')'
from 
   pdb_spfile$ f
   join v$pdbs p on (f.pdb_uid = p.con_uid)
where 
      p.name = &pdb
  and f.name  not like 'log_archive%' 
  and not regexp_like(f.name,'^_')
  and f.name not in ('local_listener','remote_listener'
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
select /* Non Hidden parameters */
   20 ord 
  ,'UNION select inst_id,name,value || case isdefault when ''FALSE'' then '' (SPFILE)'' else null end,'''|| value || ''' from gv$parameter where name = ''' || name || ''' and inst_id=' || inst_id || ' and (nvl(value,''$$@@$$'') != nvl('''||value||''',''$$@@$$'') or isdefault != '''||isdefault||''')'  a
  , '-- ----'
--  ,name
--  ,value || case isdefault when 'TRUE' then ' (Def)' else null end
from 
  gv$parameter 
where 
      (&mode='ALL' or isdefault='FALSE')
  and name  not like 'log_archive%' 
  and not regexp_like(name,'^_')
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
select /* Hidden parameters */
   30 ord 
  , 'UNION select inst_id,name,value || case isdefault when ''FALSE'' then '' (SPFILE)'' else null end,'''|| value || ''' from gv$parameter where name = ''' || name || ''' and inst_id=' || inst_id || ' and nvl(value,''$$@@$$'') != nvl('''||value||''',''$$@@$$'') '  a
  , 'UNION select '||inst_id||','''||name||''',''  *** Not Set ***'','''|| value || ''' from dual where not exists (select 1 from gv$parameter  where name = ''' || name || ''' and inst_id=' || inst_id ||') '  a
--  ,name
--  ,value || case isdefault when 'TRUE' then ' (Def)' else null end
from 
  gv$parameter 
where 
      (&mode='ALL' or isdefault='FALSE')
  and name  not like 'log_archive%' 
  and regexp_like(name,'^_')
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
   40 ord 
   , 'order by 2,1;'
   , null
--  ,null
--  ,null
from dual
/
