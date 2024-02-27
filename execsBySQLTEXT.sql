set feedback off
set serveroutput on
begin
  if (upper('&1') in ('USAGE','HELP','-?','-H') or '&1' is null)
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    heatMapOPAProcessingTime.sql SQLTEXT [start] [end] 
|
|      Extracs execitions of a specific SQLTEXT portion in the past
|
|   Parameters :
|       SQLTEXT  : SQL TEXT portion to analyze                        - Mandatory                            
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Noon (Today or yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
|       plans    : Show PLANS                                         - Default : YES
|
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/


-- -----------------------------------------------------------------
-- Parameters
-- -----------------------------------------------------------------

define SQL_TEXT='&1'
--
--  Analysis start date : Default (If before noon, noon yesterday, otherwise noon)
--
define start_date_FR="case when '&2' is null then round(sysdate)-0.5 else to_date('&2','dd/mm/yyyy hh24:mi:ss') end"
--
--  Analysis end date : default now
--
define end_date_FR="case when '&3' is null then sysdate else to_date('&3','dd/mm/yyyy hh24:mi:ss') end"
--
--  Display plans used in the period
--
define showPlans="case when '&4' is null then 'YES' else upper('&4') end"

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
col PARSING_SCHEMA_NAME format a5 trunc
col PX_SERVERS_EXECS_TOTAL format 999 heading "PxS"
col sql_id format a20
col plage_temps format a20




alter session set nls_numeric_characters=', ' ;

select
   sql.sql_id
  ,to_char(snap.BEGIN_INTERVAL_TIME,'dd/mm/yyyy hh24:mi:ss') begin_snap_time
  ,to_char(snap.END_INTERVAL_TIME,'dd/mm/yyyy hh24:mi:ss') end_snap_time
  ,to_char(snap.END_INTERVAL_TIME,'dd/mm/yyyy') day
  ,sql.PARSING_SCHEMA_NAME
  ,sql.PX_SERVERS_EXECS_TOTAL
  ,sql.disk_reads_total
  ,sql.ROWS_PROCESSED_TOTAL       rowsCnt
  ,sql.ELAPSED_TIME_TOTAL/1000000 elapsed_all
  ,case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else (sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL end elapsed_real
  ,case
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 10000 then '> 10 000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 5000 then '5 001 - 10 000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 1000 then '1001 - 5000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 500  then '501-1000 s'
    when case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else ((sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL) end > 250  then '251-500 s'
    else '<250 s'
   end plage_temps
  ,sql.PLAN_HASH_VALUE
--  ,OPTIMIZER_ENV_HASH_VALUE
from
  (select 
      sql.*
     ,txt.sql_text
   from dba_hist_sqlstat     sql
   join dba_hist_sqltext     txt on ( sql.dbid = txt.dbid and sql.sql_id=txt.sql_id)
  )sql
join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                and sql.dbid            = snap.dbid
                                and sql.instance_number = snap.instance_number
                                )
where
      upper(sql.sql_text) like '%&SQL_TEXT%'
  and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
order by BEGIN_INTERVAL_TIME
/

prompt
prompt ==================================================================================
prompt Execution times per PLAN
prompt ==================================================================================
prompt

col plan format a20
col min_time format 999G999D99
col avg_time format 999G999D99
col max_time format 999G999D99
col cnt format 999G999

select
   sql_id
  ,to_char(PLAN_HASH_VALUE) plan
  ,min(elapsed_real) min_time
  ,avg(elapsed_real) avg_time 
  ,max(elapsed_real) max_time
  ,count(PLAN_HASH_VALUE) Cnt
from (
      select
         sql.PLAN_HASH_VALUE
        ,sql.sql_id
        ,case when sql.PX_SERVERS_EXECS_TOTAL = 0 then sql.ELAPSED_TIME_TOTAL/1000000 else (sql.ELAPSED_TIME_TOTAL/1000000)/sql.PX_SERVERS_EXECS_TOTAL end elapsed_real
      from
        dba_hist_sqlstat     sql
      join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                      and sql.dbid            = snap.dbid
                                      and sql.instance_number = snap.instance_number
                                      )
      where
        sql.sql_id in ( 
                        select
                          sql.sql_id
                        from  ( select 
                                   sql2.*
                                  ,txt.sql_text
                                from 
                                   dba_hist_sqlstat     sql2
                                join dba_hist_sqltext     txt on ( sql2.dbid = txt.dbid and sql2.sql_id=txt.sql_id) ) sql
                        join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                                        and sql.dbid            = snap.dbid
                                                        and sql.instance_number = snap.instance_number
                                                       )
                        where
                              upper(sql.sql_text) like '%&SQL_TEXT%'
                          and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
                       )
        and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
     )
group by
   sql_id
  ,PLAN_HASH_VALUE
order by 3 desc
/

prompt
prompt ==================================================================================
prompt Execution plans for : &SQL_TEXT (Only those in the selection above)
prompt ==================================================================================
prompt

rem SELECT * FROM table(DBMS_XPLAN.DISPLAY_AWR('&SQL_ID'));

set serveroutput on size unlimited format wrapped feed off
declare 
  i number := 0 ;
begin
  if ( &showPlans='YES' )
  then
    for rec1 in (
                 select  DISTINCT sql.PLAN_HASH_VALUE ,sql_id 
                 from
                 dba_hist_sqlstat     sql
                 join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                                and sql.dbid            = snap.dbid
                                                and sql.instance_number = snap.instance_number
                                                )
                where
                     sql.sql_id  in (
                        select
                          sql.sql_id
                        from  ( select 
                                   sql2.*
                                  ,txt.sql_text
                                from 
                                   dba_hist_sqlstat     sql2
                                join dba_hist_sqltext     txt on ( sql2.dbid = txt.dbid and sql2.sql_id=txt.sql_id) ) sql
                        join dba_hist_snapshot snap on (    sql.snap_id         = snap.snap_id
                                                        and sql.dbid            = snap.dbid
                                                        and sql.instance_number = snap.instance_number
                                                       )
                        where
                              upper(sql.sql_text) like '%&SQL_TEXT%'
                          and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
                                    )
                 and BEGIN_INTERVAL_TIME between &start_date_FR and &end_date_FR
                )
    loop
      i := i + 1 ;
      dbms_output.put_line('=================================================================================') ;
      dbms_output.put_line('Plan : ' || rec1.PLAN_HASH_VALUE);
      dbms_output.put_line('=================================================================================') ;
    
      for rec2 in (select * from table(DBMS_XPLAN.DISPLAY_AWR(rec1.sql_id,rec1.PLAN_HASH_VALUE)) )
      loop
        dbms_output.put_line(rec2.plan_table_output) ;
      end loop ;
      dbms_output.put_line('') ;
    end loop ;
  else
    dbms_output.put_line('') ;
    dbms_output.put_line('  To display plans use ''YES'' or null as fourth parameter') ;
    dbms_output.put_line('') ;
  end if ;
end ;
/
