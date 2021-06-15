REM -------------------------------
REM Script to monitor rman backup/restore operations
REM To run from sqlplus:   @monitor '<dd-mon-rr hh24:mi:ss>'
REM Example:  
--SQL>spool monitor.out
--SQL>@monitor '06-aug-12 16:38:03'
REM where <date> is the start time of your rman backup or restore job
REM Run monitor script periodically to confirm rman is progessing
REM -------------------------------

alter session set nls_date_format='dd-mon-rr hh24:mi:ss';
set lines 1500
set pages 100
col CLI_INFO format a10
col spid format a5
col ch format a30
col seconds format 999999.99
col filename format a65
col bfc  format 9
col "% Complete" format 999.99
col event format a40
col GB_PER_S format 999D99
col LONG_WAIT_PCT format 99D99
set numwidth 10

REM Check wait events (RMAN sessions) - this is for CURRENT waits only
REM use the following for 11G+
prompt
prompt Session progess - CURRENT wait events and time in wait so far:
prompt
select inst_id, sid, CLIENT_INFO ch, seq#, event, state, wait_time_micro/1000000 seconds
from gv$session where program like '%rman%' and
wait_time = 0 and
not action is null
order by 1,3;

REM use the following for 10G
--select  inst_id, sid, CLIENT_INFO ch, seq#, event, state, seconds_in_wait secs
--from gv$session where program like '%rman%' and
--wait_time = 0 and
--not action is null;


break on report
compute sum of GB_PER_S on report

select sysdate from dual;

REM gv$session_longops (channel level)

prompt
prompt Channel progress - gv$session_longops:
prompt
select 
   s.inst_id
  ,o.sid
  ,CLIENT_INFO ch
  ,context
  ,sofar
  ,totalwork
  ,round(sofar/totalwork*100,2) "% Complete"
  ,aio.GB_PER_S
  ,aio.LONG_WAIT_PCT
FROM 
   gv$session_longops o
  ,gv$session s
  ,(select 
       inst_id
      ,sid
      ,serial
      ,100* sum (long_waits) / sum (io_count) as "LONG_WAIT_PCT"
      ,sum (effective_bytes_per_second)/1024/1024/1024 as "GB_PER_S"
   from 
     gv$backup_async_io
   group by 
      inst_id
     ,sid
     ,serial) aio
WHERE 
      opname LIKE 'RMAN%'
  AND opname NOT LIKE '%aggregate%'
  AND o.sid=s.sid
  AND totalwork != 0
  AND sofar <> totalwork
  and aio.sid = s.sid
  and aio.inst_id= s.inst_id
  and aio.serial = s.serial#
order by 
   1
  ,3;

Prompt
Prompt Overall Throughput
Prompt


SET HEAD OFF
SELECT 'Throughput: '||
       ROUND(SUM(v.value/1024/1024),1) || ' Meg so far @ ' ||
       ROUND(SUM(v.value     /1024/1024)/NVL((SELECT MIN(elapsed_seconds)
            FROM v$session_longops
            WHERE opname          LIKE 'RMAN: aggregate input'
              AND sofar           != TOTALWORK
              AND elapsed_seconds IS NOT NULL
       ),SUM(v.value     /1024/1024)),2) || ' Meg/sec'
 FROM gv$sesstat v, v$statname n, gv$session s
WHERE v.statistic# = n.statistic#
  AND n.name       = 'physical write total bytes'
  AND v.sid        = s.sid
  AND v.inst_id    = s.inst_id
  AND s.program LIKE 'rman@%'
GROUP BY n.name
/
SET HEAD ON
