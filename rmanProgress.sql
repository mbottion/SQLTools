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
col ch format a20
col seconds format 999999.99
col filename format a65
col bfc  format 9
col "% Complete" format 999.99
col event format a40
set numwidth 10

select sysdate from dual;

REM gv$session_longops (channel level)

prompt
prompt Channel progress - gv$session_longops:
prompt
select s.inst_id, o.sid, CLIENT_INFO ch, context, sofar, totalwork,
                    round(sofar/totalwork*100,2) "% Complete"
     FROM gv$session_longops o, gv$session s
     WHERE opname LIKE 'RMAN%'
     AND opname NOT LIKE '%aggregate%'
     AND o.sid=s.sid
     AND totalwork != 0
     AND sofar <> totalwork;

REM Check wait events (RMAN sessions) - this is for CURRENT waits only
REM use the following for 11G+
prompt
prompt Session progess - CURRENT wait events and time in wait so far:
prompt
select inst_id, sid, CLIENT_INFO ch, seq#, event, state, wait_time_micro/1000000 seconds
from gv$session where program like '%rman%' and
wait_time = 0 and
not action is null;

REM use the following for 10G
--select  inst_id, sid, CLIENT_INFO ch, seq#, event, state, seconds_in_wait secs
--from gv$session where program like '%rman%' and
--wait_time = 0 and
--not action is null;
