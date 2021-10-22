set serveroutput on
declare 
  n number ;
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    dbms_output.put_line('
+---------------------------------------------------------------------------------------
| Usage:
|    spaceUsage.sql 
|   
|   Space anamysis for a database
|
|   Parameters :
|       
+---------------------------------------------------------------------------------------
       ');
   raise value_error ;
  end if ;
end ;
/

-- ------------------------------------------------------------------------
-- Parameters 
-- ------------------------------------------------------------------------

column tablespace_name          format a30          heading "Tablespace"
column contents                 format a20    trunc heading "Type"
column file_name                format a150   trunc heading "File Name"
column segment_space_management format a10           heading "Ssm"
column bigfile                  format a5           heading "Bigfile?"
column CURRENT_SIZE_GB          format 999G999D99   heading "Curr Size GB"
column MAX_SIZE_GB              format 999G999D99   heading "Max Size GB"
column REMAIN_SIZE_GB           format 999G999D99   heading "Remaining Space GB"

compute sum of CURRENT_SIZE_GB on report

select
   ts.tablespace_name
  ,ts.contents
  ,ts.segment_space_management
  ,ts.bigfile
  ,df.bytes/1024/1024/1024 CURRENT_SIZE_GB
  ,decode(df.autoextensible,'NO',df.bytes,'YES',df.maxbytes)/1024/1024/1024 MAX_SIZE_GB
  ,decode(df.autoextensible,'NO',df.bytes,'YES',df.maxbytes)/1024/1024/1024 - df.bytes/1024/1024/1024 REMAIN_SIZE_GB
  ,df.file_name
from 
  dba_data_files df
  join dba_tablespaces ts on  (df.tablespace_name = ts.tablespace_name)
order by
  decode(ts.contents,'PERMANENT',1,'UNDO',2,'TEMPORARY',3,4)
  ,ts.tablespace_name
/
