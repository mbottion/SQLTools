
SELECT
  ge.sid
 ,ge.serial#
 ,ge.start_time
 ,ge.opname
 ,to_char(ge.sofar,'999G999G999G999G999') || ' /' || to_char(ge.totalwork,'999G999G999G999G999') || ' ' ||  ge.units || ' (' || to_char(((ge.sofar/ge.totalwork)*100),'99D99') || ' % )' progress
 ,case 
    when t.ts# is null then f.name
    else t.name
  end Current_object
 ,ge.timestamp
-- ,ge.message
-- ,ge.*
FROM
  gv$session_longops ge
  left join v$tablespace t on (ge.target = t.ts#)
  left join v$datafile f on (ge.target = f.file#)
WHERE
  ge.opname LIKE 'TDE%'
  and ge.time_remaining != 0
ORDER BY
  ge.timestamp;
