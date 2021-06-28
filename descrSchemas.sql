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
|    descrSchemas.sql 
|   
|
|   Parameters :
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Noon (Today or yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
|       mode     : Groups on start_engine or end engine date (IN/OUT) - Defult  : IN (Injection Rate)
|       interval : Interval wideness (in seconds)                     - Default : 3600
|       engine   : Engine name                                        - Default : %
|       
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/
set pause off
set feed off
col db_name new_value db_name
set termout off

select
  name db_name
from 
  v$database ;

set term off


set linesize  132
set pagesize  120
set trimspool on
prompt Base de données
prompt ===============

select
   name                                       "Nom"
  ,to_char (created,'dd/mm/yyyy hh24:mi:ss')  "Création"
  ,log_mode                                   "Log mode"
  ,db_unique_name                             "Nom unique"
from
  v$database
/

prompt
prompt Instances
prompt =========

select
   INSTANCE_NAME                              "Nom"
  ,HOST_NAME                                  "Hôte"
from
  gv$instance
/

prompt
prompt Liste des schémas
prompt =================

column USERNAME             format a25 heading "Nom"           
column ACCOUNT_STATUS       format a10 heading "Status"        
column DEFAULT_TABLESPACE   format a20 heading "Default TBS"   
column TEMPORARY_TABLESPACE format a20 heading "Temporary TBS" 
column DAT                  format a20 heading "Création"      



select
   USERNAME                                    
  ,ACCOUNT_STATUS                              
  ,DEFAULT_TABLESPACE                          
  ,TEMPORARY_TABLESPACE                        
  ,to_char (created,'dd/mm/yyyy hh24:mi:ss') dat
from
  dba_users 
where 
  oracle_maintained='N'
order by
  username
/

set newpage 0
clear columns

select 
  * 
from (
    select
       100                                                                ORD
      ,'Table'                                                            OBJECT_TYPE
      ,t.owner                                                            OWNER
      ,t.table_name                                                       TABLE_NAME
      ,null                                                               INDEX_NAME
      ,t.temporary                                                        TEMPORARY
      ,null                                                               PARTITION_NAME
      ,null                                                               SUB_PART_COUNT
      ,null                                                               OBJECT_SUB_TYPE
      ,null                                                               INDEX_UNIQUENESS
      ,t.compression                                                      COMPRESSION
      ,t.compress_for                                                     COMPRESSION_FOR
      ,t.status                                                           STATUS
      ,t.num_rows                                                         NUM_ROWS
      ,null                                                               INDEX_KEYS
      ,t.degree                                                           DEGREE
      ,t.partitioned                                                      PARTITIONED
      ,to_char(t.last_analyzed,'dd/mm/yyyy hh24:mi:ss')                   LAST_ANALYZED
      ,tc.column_id                                                       COLUMN_POSITION
      ,tc.column_name                                                     COLUMN_NAME
      ,rpad(tc.data_type,15) ||
        case
          when tc.data_type='NUMBER' 
          then '('  || lpad(to_char(tc.data_precision),4) || 
                ',' || lpad(to_char(tc.data_scale),2) ||
               ')'
          when tc.data_type = 'CLOB'
          then ''
          when tc.data_type = 'LOB'
          then ''
          when tc.data_type like 'TIMESTAMP%'
          then ''
          else      '(' || lpad(to_char(tc.data_length),4) || '  )'           
        end descend_or_data_type
    from
      dba_tables t
      join dba_tab_columns tc on (    t.owner=tc.owner
                                  and t.table_name = tc.table_name)
    where
      t.owner in (select username from dba_users where oracle_maintained='N')
    UNION
    select
       110
      ,'Table partition'
      ,tp.table_owner
      ,t.table_name
      ,null
      ,t.temporary                                            
      ,tp.partition_name
      ,tp.subpartition_count
      ,null
      ,null
      ,tp.compression
      ,tp.compress_for                                    
      ,null --tp.status
      ,tp.num_rows
      ,null
      ,t.degree
      ,null
      ,to_char(tp.last_analyzed,'dd/mm/yyyy hh24:mi:ss')
      ,null
      ,null
      ,null
    from
      dba_tab_partitions tp
      join dba_tables t on (     t.owner=tp.table_owner
                             and t.table_name = tp.table_name)
    where
      tp.table_owner in (select username from dba_users where oracle_maintained='N')
    UNION
    select
       200 ord
      ,'Index' Obj_type
      ,i.owner
      ,i.table_name
      ,i.index_name
      ,i.temporary                                                       
      ,'N/A' PARTITION_NAME
      ,null
      ,i.index_type
      ,i.uniqueness
      ,i.compression
      ,null                                 
      ,i.status
      ,i.num_rows
      ,i.distinct_keys
      ,i.degree
      ,i.partitioned
      ,to_char(i.last_analyzed,'dd/mm/yyyy hh24:mi:ss')
      ,ic.column_position
      ,ic.column_name
      ,ic.descend
    from
      dba_indexes i
      join dba_ind_columns ic on (    i.owner=ic.index_owner
                                  and i.index_name = ic.index_name)
    where
      i.owner in (select username from dba_users where oracle_maintained='N')
    union
    select
       210
      ,'Index partition' Obj_type
      ,ip.index_owner
      ,i.table_name
      ,i.index_name
      ,i.temporary                                        
      ,ip.partition_name
      ,ip.subpartition_count
      ,i.index_type
      ,i.uniqueness
      ,ip.compression
      ,null                                                  
      ,ip.status
      ,ip.num_rows
      ,ip.distinct_keys
      ,i.degree
      ,null
      ,to_char(ip.last_analyzed,'dd/mm/yyyy hh24:mi:ss')
      ,null
      ,null
      ,null
    from
      dba_ind_partitions ip
      join dba_indexes i on (    i.owner=ip.index_owner
                             and i.index_name = ip.index_name)
    where
      ip.index_owner in (select username from dba_users where oracle_maintained='N')
  )
where 
  &owner_where
order by
   table_name
  ,ord
  ,index_name
  ,partition_name
  ,column_position
/
