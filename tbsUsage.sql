set linesize 132 tab off trimspool on
set pagesize 105
set pause off
set echo off
set feed off
alter session set nls_numeric_characters=', ' ;
set feedb on



column "TOTAL ALLOC (MB)" format 9G999G999G990D00
column "TOTAL PHYS ALLOC (MB)" format 9G999G999G990D00
column "USED (MB)" format  9G999G999G990D00
column "FREE (MB)" format 9G999G999G990D00
column "% USED" format 990G00

select a.tablespace_name, c.contents,
       a.bytes_alloc/(1024*1024) "TOTAL ALLOC (MB)",
       a.physical_bytes/(1024*1024) "TOTAL PHYS ALLOC (MB)",
       nvl(b.tot_used,0)/(1024*1024) "USED (MB)",
       (nvl(b.tot_used,0)/a.bytes_alloc)*100 "% USED"
from ( select tablespace_name,
       sum(bytes) physical_bytes,
       sum(decode(autoextensible,'NO',bytes,'YES',maxbytes)) bytes_alloc
       from (      SELECT  file_name ,tablespace_name ,bytes ,autoextensible, maxbytes FROM    dba_data_files
             UNION SELECT  file_name ,tablespace_name ,bytes ,autoextensible, maxbytes FROM    dba_temp_files)
       group by tablespace_name ) a,
     ( select tablespace_name, sum(bytes) tot_used
       from dba_segments
       group by tablespace_name ) b
     ,dba_tablespaces c
where a.tablespace_name = b.tablespace_name (+)
and   a.tablespace_name = c.tablespace_name
--and   (nvl(b.tot_used,0)/a.bytes_alloc)*100 > 10
--and   a.tablespace_name not in (select distinct tablespace_name from dba_temp_files)
--and   a.tablespace_name not like 'UNDO%'
order by decode (c.contents,'UNDO',10,'TEMPORARY',20,1),a.tablespace_name
--order by 5
/
