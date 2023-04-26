set feedback off
set serveroutput on
begin
  if (upper('&1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|   stmtAnalysis_duration.sql [start] [end] 
|
|       Statements duration (from gv$active_session_history) (precision= 1s). For a specific SQL_ID
|
|   Parameters :
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Noon (Today or yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
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

--
--   Used in PIVOT 2
--
define long_running_time=3600

set pages 0 head off
col INFORMATION format a100 newline

select
   ''                                                                              INFORMATION
  ,'=============================================================================' INFORMATION
  ,'SQL Statement duration analysis'                                               INFORMATION
  ,'=============================================================================' INFORMATION
  ,''                                                                              INFORMATION
  ,'  Duration of &SQL_ID per plan hash       '                                    INFORMATION
  ,''                                                                              INFORMATION
  ,'  between ' || to_char(&start_date_FR,'dd/mm/yyyy hh24:mi:ss') || 
      ' and ' || to_char(&end_date_FR,'dd/mm/yyyy hh24:mi:ss')                     INFORMATION
  ,''                                                                              INFORMATION
  ,'  Oldest session available : ' || (select to_char(min(sample_time),'dd/mm/yyyy hh24:mi:ss') from gv$active_session_history) INFORMATION
  ,''                                                                              INFORMATION
  ,'=============================================================================' INFORMATION
  ,''                                                                              INFORMATION
from
  dual ;
set head on
set pages 10000


col instance_number format 999     heading "Inst."
col username        format a30     heading "User"
col C1              format 999G999 heading "0-2 secs"
col C2              format 999G999 heading "2-5 secs"
col C3              format 999G999 heading "5-10 secs"
col C4              format 999G999 heading "10-30 secs"
col C5              format 999G999 heading "30-60 secs"
col C6              format 999G999 heading "1-2 mins"
col C7              format 999G999 heading "2-5 mins"
col C8              format 999G999 heading "5-10 mins"
col C9              format 999G999 heading "10-30 mins"
col C10             format 999G999 heading "30-60 mins"
col C11             format 999G999 heading "> 1 h"
col SQL_ID_HASH     format a35     heading "SQL ID/PLan Hash"
col sql_text        format a50     word_wrapped

break on report
compute sum of C1  on report 
compute sum of c2  on report
compute sum of c3  on report
compute sum of c4  on report
compute sum of c5  on report
compute sum of c6  on report
compute sum of c7  on report
compute sum of c8  on report
compute sum of c9  on report
compute sum of c10 on report
compute sum of c11  on report


with running_duration as (
    select /*+PARALLEL(8)*/
      --
      --    Collect the time taken by each session to run a SQLID
      --    Precision is +/- 10 secs since there is one line in DBA_HIST_ACTIVE_SESSION_HISTORY
      --    every 10 seconds for each running session.
      --
      --    For a better precision, we can build a similar statement using v$active_session_history 
      --    which keeps a track of each session every second, but the number of lines may not
      --    cover a full day
      --
       sh.inst_id instance_number
      ,rpad(du.username,20) || case 
                                 when s.audsid is null then ''
                                 else ' (Running)'
                               end username
      ,sh.sql_id
      ,sh.sql_exec_id
      --
      -- Duration is the difference between the time a statement is first seen, and its last apparition for a given EXEC_ID
      --
      ,(cast(sample_time as date) - cast(min(sh.sample_time) over (partition by sh.inst_id,sh.user_id,sh.sql_id,sh.sql_exec_id) as date))*3600*24 duration_secs
      --
      --   Informational, just in case
      --
      ,min(sh.sample_time) over (partition by sh.inst_id,sh.user_id,sh.sql_id,sh.sql_exec_id) start_time
      ,sh.sample_time
      ,sh.program
      ,sh.sql_plan_hash_value
    from 
      gv$active_session_history sh
      --join dba_hist_snapshot ds on (ds.snap_id = sh.snap_id and ds.instance_number = sh.instance_number)
      join dba_users du on (sh.user_id = du.user_id)
      --
      --    To diff?rentiate running statements
      --
      left join gv$session s on (    s.sid=sh.session_id 
                                 and s.serial#=sh.session_serial# 
                                 and s.sql_id=sh.sql_id 
                                 and s.sql_exec_id = sh.sql_exec_id
                                 and s.inst_id = sh.inst_id)
    where 
          sh.session_type='FOREGROUND'          -- Only user sessions
      and du.username not in ('SYS','SYSTEM')   -- Filter unneeded users
      and sh.sample_time between &start_date_FR and &end_date_FR
      and sh.sql_id is not null                 -- Sessions qui n'ont rien ex?cut??
      and sh.sql_exec_id is not null            -- Donnent des r?sultats bizarres
)
,stmt_duration as (
    select
       --
       --   Collect interresting lines from the select above
       --
       instance_number
      ,username
      ,sql_id
      ,sql_exec_id
      ,max(duration_secs) duration_secs
    from 
      running_duration
    group by
       instance_number
      ,username
      ,sql_id
      ,sql_exec_id
)
,stmt_duration_hash as (
    select
       --
       --   Collect interresting lines from the select above (same than stmt_duration, but group also by sql_plan_hash_value)
       --
       instance_number
      ,username
      ,sql_id
      ,sql_exec_id
      ,sql_plan_hash_value
      ,max(duration_secs) duration_secs
    from 
      running_duration
    group by
       instance_number
      ,username
      ,sql_id
      ,sql_exec_id
      ,sql_plan_hash_value
)
,stmt_duration_pre_pivot as (
     select 
        --
        --    Prepare data for the PIVOT
        --
        instance_number
       ,username
       ,case
         when duration_secs between 0          and 2         then '0-2 secs'
         when duration_secs between 2          and 5         then '2-5 secs'
         when duration_secs between 5          and 10        then '5-10 secs'
         when duration_secs between 10         and 30        then '10-30 secs'
         when duration_secs between 30         and 60        then '30-60 secs'
         when duration_secs between 60         and (2*60)    then '1-2 mins'
         when duration_secs between (2*60)     and (5*60)    then '2-5 mins'
         when duration_secs between (5*60)     and (10*60)   then '5-10 mins'
         when duration_secs between (10*60)    and (30*60)   then '10-30 mins'
         when duration_secs between (30*60)    and (60*60)   then '30-60 mins'
         else '> 1 h'
        end duration_interval 
       ,sql_id
       ,duration_secs
     from 
       stmt_duration
     where 
       --
       --   To fiter long running statement , add criteria here
       --
       duration_secs >=0
     order by instance_number,username
)       
,stmt_duration_pre_pivot_hash as (
     select 
        --
        --    Prepare data for the PIVOT (Same than above but add sql_plan_hash_value)
        --
        instance_number
       ,username
       ,case
         when duration_secs between 0          and 2         then '0-2 secs'
         when duration_secs between 2          and 5         then '2-5 secs'
         when duration_secs between 5          and 10        then '5-10 secs'
         when duration_secs between 10         and 30        then '10-30 secs'
         when duration_secs between 30         and 60        then '30-60 secs'
         when duration_secs between 60         and (2*60)    then '1-2 mins'
         when duration_secs between (2*60)     and (5*60)    then '2-5 mins'
         when duration_secs between (5*60)     and (10*60)   then '5-10 mins'
         when duration_secs between (10*60)    and (30*60)   then '10-30 mins'
         when duration_secs between (30*60)    and (60*60)   then '30-60 mins'
         else '> 1 h'
        end duration_interval 
       ,sql_id
       ,sql_plan_hash_value
       ,duration_secs
     from 
       stmt_duration_hash
     where 
       --
       --   To fiter long running statement , add criteria here
       --
       duration_secs >=0
     order by instance_number,username
)       
/* -------------------------------------------------------------
 *    To see rougth data
 * ------------------------------------------------------------- 
 * /
select * from running_duration
where sql_id in ('dk2s1w258j6s5','dm8bxtj5r1vwj')
order by 1,2,3,4,6
/* -------------------------------------------------------------
 *    To see useful data
 * ------------------------------------------------------------- 
 * /
select * from stmt_duration 
where sql_id in ('dk2s1w258j6s5','dm8bxtj5r1vwj')
/* -------------------------------------------------------------
 *    To see data for the pivot
 * ------------------------------------------------------------- 
 * /
select * from stmt_duration_pre_pivot 
where sql_id in ('dk2s1w258j6s5','dm8bxtj5r1vwj')
/* -------------------------------------------------------------
 *    To see sql text
 * ------------------------------------------------------------- 
 * /
select sd.*,dt.sql_text 
from stmt_duration sd
join dba_hist_sqltext dt on (dt.sql_id = sd.sql_id)
where sd.duration_secs >= 10
order by 1,2
/**************************************************************
 *
 *     Below are the different useful pivots to present
 * the results in various ways. To use one, simply remove
 * the space between star and slash in the comment 
 *
 **************************************************************/
/* ------------------------------------------------------------- 
 * PIVOT 1 
 * =============================================================
 *    Final statement to show data in a pivoted format
 *    count of statement in interval per
 *       INSTANCE
 *       USER
 * ------------------------------------------------------------- 
 * /
select * from (select 
                  instance_number
                 ,username
                 ,duration_interval 
               from 
                  stmt_duration_pre_pivot
              )
pivot ( count(duration_interval) for duration_interval in ( 
              '0-2 secs'    as C1  , '2-5 secs'    as C2  , '5-10 secs'   as C3
             ,'10-30 secs'  as C4  , '30-60 secs'  as C5  , '1-2 mins'    as C6  , '2-5 mins' as C7
             ,'5-10 mins'   as C8  , '10-30 mins'  as C9  , '30-60 mins'  as C10
             ,'> 1 h'       as C11
             )
      )
order by 1, 2
/* ------------------------------------------------------------- 
 * PIVOT 2 
 * =============================================================
 *    Final statement to show data in a pivoted format
 *    count of runs in each interval per
 *       SQLID
 *
 *    Only statements for which at least one execution lasted 
 * more then long_running_duration' seconds
 * ------------------------------------------------------------- 
 * /
select * from (select
                  s1.sql_id
                 ,cast(dt.sql_text as varchar2(2000)) sql_text
                 ,s1.duration_interval 
               from 
                  stmt_duration_pre_pivot s1
               join dba_hist_sqltext dt on (dt.sql_id = s1.sql_id)
               where
               exists (select 
                         1 
                       from 
                         stmt_duration_pre_pivot s2
                       where
                             s1.sql_id = s2.sql_id
                         and s2.duration_secs >= &long_running_time
                        )
              )
pivot ( count(duration_interval) for duration_interval in ( 
              '0-2 secs'    as C1  , '2-5 secs'    as C2  , '5-10 secs'   as C3
             ,'10-30 secs'  as C4  , '30-60 secs'  as C5  , '1-2 mins'    as C6  , '2-5 mins' as C7
             ,'5-10 mins'   as C8  , '10-30 mins'  as C9  , '30-60 mins'  as C10
             ,'> 1 h'       as C11
             )
      )
/* ------------------------------------------------------------- 
 * PIVOT 3 
 * =============================================================
 *    Show durations for a specific SQLID
 * ------------------------------------------------------------- 
 */
select * from (select
                  s1.sql_id||'/'||s1.sql_plan_hash_value SQL_ID_HASH
                 ,s1.duration_interval 
               from 
                  stmt_duration_pre_pivot_hash s1
               join dba_hist_sqltext dt on (dt.sql_id = s1.sql_id)
               where
               exists (select 
                         1 
                       from 
                         stmt_duration_pre_pivot_hash s2
                       where
                             s1.sql_id = s2.sql_id
                        )
              and s1.sql_id = '&sql_ID'
              )
pivot ( count(duration_interval) for duration_interval in ( 
              '0-2 secs'    as C1  , '2-5 secs'    as C2  , '5-10 secs'   as C3
             ,'10-30 secs'  as C4  , '30-60 secs'  as C5  , '1-2 mins'    as C6  , '2-5 mins' as C7
             ,'5-10 mins'   as C8  , '10-30 mins'  as C9  , '30-60 mins'  as C10
             ,'> 1 h'       as C11
             )
      )
order by 1, 2
/* ************************************************************************** */
/
