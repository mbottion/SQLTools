set feedback off
set serveroutput on
begin
  if (upper('&1') in ('USAGE','HELP','-?','-H') or '&1' is null)
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    heatMapOPAProcessingTime.sql SQLID [start] [end] 
|
|      Extracs execitions of a specific SQLID in the past
|
|   Parameters :
|       SQLID    : SQL to analyze                                     - Mandatory                            
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Noon (Today or yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
|       engine   : Engine name                                        - Default : %
|       Type     : Type of display VALUES/PCT                         - Default : PCT
|       interval : interval of cases number used to group the results - Default : 50
|
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/


-- -----------------------------------------------------------------
-- Parameters
-- -----------------------------------------------------------------

define SQL_ID='&1'
--
--  Analysis start date : Default (If before noon, noon yesterday, otherwise noon)
--
define start_date_FR="case when '&2' is null then round(sysdate)-0.5 else to_date('&2','dd/mm/yyyy hh24:mi:ss') end"
--
--  Analysis end date : default now
--
define end_date_FR="case when '&3' is null then sysdate else to_date('&3','dd/mm/yyyy hh24:mi:ss') end"

set lines 200
set pages 10000
set tab off
col begin_snap_time format a20
col end_snap_time format a20
col PX_SERVERS_EXECS_TOTAL format 999999
col rowsCnt format 999G999G999G999
col disk_reads_total format 999G999G999G999
col elapsed_all format 999G999G999D99
col elapsed_real format 999G999G999D99
col PARSING_SCHEMA_NAME format a10
col plage_temps format a20




alter session set nls_numeric_characters=', ' ;

select
   to_char(snap.BEGIN_INTERVAL_TIME,'dd/mm/yyyy hh24:mi:ss') begin_snap_time
  ,to_char(snap.END_INTERVAL_TIME,'dd/mm/yyyy hh24:mi:ss') end_snap_time
  ,to_char(snap.END_INTERVAL_TIME,'dd/mm/yyyy') day
  ,sql.PARSING_SCHEMA_NAME
  ,sql.PX_SERVERS_EXECS_TOTAL
  ,sql.disk_reads_total
  ,sql.ROWS_PROCESSED_TOTAL       rowsCnt
  ,sql.ELAPSED_TIME_TOTAL/1000000 elapsed_all
  ,case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else (sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL end elapsed_real
  ,case 
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 10000 then '> 10 000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 5000 then '5 001 - 10 000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 1000 then '1001 - 5000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 500  then '501-1000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then null else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 250  then '251-500 s'
    else '<250 s'
   end plage_temps
  ,sql.PLAN_HASH_VALUE
--  ,OPTIMIZER_ENV_HASH_VALUE
from
  dba_hist_sqlstat     sql
join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                and sql.dbid            = snap.dbid
                                and sql.instance_number = snap.instance_number
                                )
where 
  sql.sql_id = '&SQL_ID' 
             and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
order by BEGIN_INTERVAL_TIME
/


prompt
prompt ==================================================================================
prompt Execution plans for : &SQL_ID (Only those in the selection above)
prompt ==================================================================================
prompt

rem SELECT * FROM table(DBMS_XPLAN.DISPLAY_AWR('&SQL_ID'));

set serveroutput on size unlimited format wrapped feed off
declare 
  i number := 0 ;
begin
  for rec1 in (
               select  DISTINCT sql.PLAN_HASH_VALUE  
               from
               dba_hist_sqlstat     sql
               join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                              and sql.dbid            = snap.dbid
                                              and sql.instance_number = snap.instance_number
                                              )
              where
                   sql.sql_id = '&SQL_ID'
               and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
              )
  loop
    i := i + 1 ;
    dbms_output.put_line('=================================================================================') ;
    dbms_output.put_line('Plan : ' || rec1.PLAN_HASH_VALUE);
    dbms_output.put_line('=================================================================================') ;
    
    for rec2 in (select * from table(DBMS_XPLAN.DISPLAY_AWR('&SQL_ID',rec1.PLAN_HASH_VALUE)) )
    loop
      dbms_output.put_line(rec2.plan_table_output) ;
    end loop ;
    dbms_output.put_line('') ;
  end loop ;
  dbms_output.put_line('') ;
  dbms_output.put_line('') ;
  dbms_output.put_line('===================================================================================================================') ;
  dbms_output.put_line('NOTE : ' || i || ' different plans observed for &SQL_ID between ' || &start_date_FR || ' and ' || &end_date_FR ) ;
  dbms_output.put_line('===================================================================================================================') ;
  dbms_output.put_line('') ;
end ;
/
