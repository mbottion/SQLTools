col instance_number format 999     heading "Inst."
col username        format a30     heading "User"
col C1              format 999G999 heading "<=10 secs"
col C2              format 999G999 heading "10-30 secs"
col C3              format 999G999 heading "30-60 secs"
col C4              format 999G999 heading "1-5 mins"
col C5              format 999G999 heading "5-10 mins"
col C6              format 999G999 heading "10-30 mins"
col C7              format 999G999 heading "30-60 mins"
col C8              format 999G999 heading "1-2 h"
col C9              format 999G999 heading "2-6 h"
col C10             format 999G999 heading "6-12 h"
col C11             format 999G999 heading "> 12 h"
col C12             format 999G999 heading "<=10 secs"
col C1              format 999G999 heading "<=10 secs"
col C1              format 999G999 heading "<=10 secs"
col C1              format 999G999 heading "<=10 secs"

col sql_text        format a50     word_wrapped

define long_running_time=3600

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
       ds.instance_number
      ,rpad(du.username,20) || case 
                                 when s.audsid is null then ''
                                 else ' (Running)'
                               end username
      ,sh.sql_id
      ,sh.sql_exec_id
      --
      -- Duration is the difference between the time a statement is first seen, and its last apparition for a given EXEC_ID
      --
      ,(cast(sample_time as date) - cast(min(sh.sample_time) over (partition by ds.instance_number,sh.user_id,sh.sql_id,sh.sql_exec_id) as date))*3600*24 duration_secs
      --
      --   Informational, just in case
      --
      ,min(sh.sample_time) over (partition by ds.instance_number,sh.user_id,sh.sql_id,sh.sql_exec_id) start_time
      ,sh.sample_time
      ,sh.program
      ,sh.sql_plan_hash_value
    from 
      dba_hist_active_sess_history sh
      join dba_hist_snapshot ds on (ds.snap_id = sh.snap_id and ds.instance_number = sh.instance_number)
      join dba_users du on (sh.user_id = du.user_id)
      --
      --    To diff?rentiate running statements
      --
      left join gv$session s on (    s.sid=sh.session_id 
                                 and s.serial#=sh.session_serial# 
                                 and s.sql_id=sh.sql_id 
                                 and s.sql_exec_id = sh.sql_exec_id
                                 and s.inst_id = sh.instance_number)
    where 
          sh.session_type='FOREGROUND'          -- Only user sessions
      and du.username not in ('SYS','SYSTEM')   -- Filter unneeded users
      and sh.sample_time > trunc(sysdate)       -- Current day
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
       --   Collect interresting lines from the select above
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
         when duration_secs between 0          and 10        then '<=10 secs'
         when duration_secs between 10         and 30        then '10-30 secs'
         when duration_secs between 30         and 60        then '30-60 secs'
         when duration_secs between 60         and (5*60)    then '1-5 mins'
         when duration_secs between (5*60)     and (10*60)   then '5-10 mins'
         when duration_secs between (10*60)    and (30*60)   then '10-30 mins'
         when duration_secs between (30*60)    and (60*60)   then '30-60 mins'
         when duration_secs between (3600)     and (2*3600)  then '1-2 h'
         when duration_secs between (2*3600)   and (6*3600)  then '2-6 h'
         when duration_secs between (6*3600)   and (12*3600) then '6-12 h'
         else '> 12 h'
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
        --    Prepare data for the PIVOT
        --
        instance_number
       ,username
       ,case
         when duration_secs between 0          and 10        then '<=10 secs'
         when duration_secs between 10         and 30        then '10-30 secs'
         when duration_secs between 30         and 60        then '30-60 secs'
         when duration_secs between 60         and (5*60)    then '1-5 mins'
         when duration_secs between (5*60)     and (10*60)   then '5-10 mins'
         when duration_secs between (10*60)    and (30*60)   then '10-30 mins'
         when duration_secs between (30*60)    and (60*60)   then '30-60 mins'
         when duration_secs between (3600)     and (2*3600)  then '1-2 h'
         when duration_secs between (2*3600)   and (6*3600)  then '2-6 h'
         when duration_secs between (6*3600)   and (12*3600) then '6-12 h'
         else '> 12 h'
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
              '<=10 secs' as C1  , '10-30 secs' as C2  , '30-60 secs' as C3
             ,'1-5 mins'  as C4  , '5-10 mins'  as C5  , '10-30 mins' as C6  , '30-60 mins' as C7
             ,'1-2 h'     as C8  , '2-6 h'      as C9  , '6-12 h'     as C10
             ,'> 12 h'    as C11
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
               and s1.sql_id='0bgtzyr8nsu8t'
              )
pivot ( count(duration_interval) for duration_interval in ( 
              '<=10 secs' as C1  , '10-30 secs' as C2  , '30-60 secs' as C3
             ,'1-5 mins'  as C4  , '5-10 mins'  as C5  , '10-30 mins' as C6  , '30-60 mins' as C7
             ,'1-2 h'     as C8  , '2-6 h'      as C9  , '6-12 h'     as C10
             ,'> 12 h'    as C11
             )
      )
order by 1, 2
/* ************************************************************************** */
/

select * from dba_hist_active_sess_history ;