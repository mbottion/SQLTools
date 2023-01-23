set line 200
 col PARAMETER format a40
 col DESCRIPTION format a60
 col SESSION_VALUE format a10
 col INSTANCE_VALUE format a10
 col NUM format 9999
 
 SELECT a.ksppinm AS parameter,
        a.INDX    as num,
        a.ksppdesc AS description,
        b.ksppstvl AS session_value,
        c.ksppstvl AS instance_value
 FROM   x$ksppi a,
        x$ksppcv b,
        x$ksppsv c
 WHERE  a.indx = b.indx
 AND    a.indx = c.indx
 AND    a.ksppinm LIKE '/_%' ESCAPE '/'
 ORDER BY a.ksppinm;
