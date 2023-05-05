set lines 200
set pages 200
col module format a50
col elapsed_seconds format 999G999G990
col Progress format a10
col action format a20
col opname format a40
col inst_id format 99
col sid format 9999999
col serial# format 999999

SELECT
   s.module
  ,l.elapsed_seconds
  ,l.time_remaining
  ,to_char((l.sofar/l.totalwork)*100,'990D00') || ' %' "Progress"
  ,s.action
  ,l.opname
  ,s.inst_id
  ,s.sid
  ,s.serial#
--  ,l.sql_plan_operation
--  ,l.qcsid
--  ,l.*
--  ,s.*
FROM
  gv$session_longops l
join gv$session s on (    s.inst_id = l.inst_id
                      and s.sid=l.sid
                      and s.serial# = l.serial# )
WHERE
  time_remaining > 0
order by s.module;
