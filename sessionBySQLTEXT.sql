Rem
Rem     Find a session running a given SQL
Rem
SELECT
  ss.inst_id
 ,ss.sid
 ,ss.serial#
 ,ss.username
 ,ss.process
 ,ss.program
 ,ss.machine
 ,sq.last_active_time
 ,sq.sql_fulltext
 ,sq.sql_id
FROM
  sys.gv$session ss
  JOIN sys.gv$open_cursor oc
  ON ss.sid       = oc.sid
     AND ss.inst_id   = oc.inst_id
  JOIN sys.gv$sql sq
  ON sq.sql_id    = oc.sql_id
     AND sq.inst_id   = oc.inst_id
WHERE
  sq.users_executing > 0
  AND oc.sql_text LIKE '%RES_MODALITE_CONTRACTUELLE_IX2%';
