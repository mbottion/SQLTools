define P1="&1"
define P2="&2"

rem
rem  Reference 
rem
rem  https://dba.stackexchange.com/questions/6593/simplified-automated-datafile-free-disk-space-reclaim
rem

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

set linesize 500 trimspool on trimout on tab off pagesize 10000

prompt
prompt =========================================================================
Prompt .         Actual and maximum sizes of datafiles, possible extentions for
Prompt each file, plus space really used by extents in each file.
prompt =========================================================================
Prompt

column tablespace_name          format a30          heading "Tablespace"
column contents                 format a20    trunc heading "Type"
column file_name                format a50    trunc heading "File Name"
column segment_space_management format a10           heading "Spc Mgmt"
column bigfile                  format a5           heading "Bigfile?"
column CURRENT_SIZE_GB          format 999G990D99   heading "Curr Size GB"
column MAX_SIZE_GB              format 999G990D99   heading "Max Size GB"
column POSSIBLE_EXTENTION_GB    format 999G990D99   heading "Possible extension GB"
column USED_BY_SEGMENTS_GB      format 999G990D99   heading "Used by segments GB"
column FREE_SPACE_GB            format 999G990D99   heading "Free space GB"
column FREE_SPACE_PCT           format 990D99       heading "Free space %"

break on tablespace_name on contents on segment_space_management on bigfile on report
rem compute sum of CURRENT_SIZE_GB on tablespace_name
compute sum of CURRENT_SIZE_GB on report
rem compute sum of MAX_SIZE_GB on tablespace_name
compute sum of MAX_SIZE_GB on report
compute sum of used_by_segments_gb on report
compute sum of free_space_gb on report


with detail_data as (
    select
       ts.tablespace_name
      ,ts.contents
      ,ts.segment_space_management
      ,ts.bigfile
      ,df.file_id
      ,df.bytes/1024/1024/1024 CURRENT_SIZE_GB
      ,us.tot_used/1024/1024/1024 USED_BY_SEGMENTS_GB
      ,decode(df.autoextensible,'NO',df.bytes,'YES',df.maxbytes)/1024/1024/1024 MAX_SIZE_GB
      ,df.file_name
--      ,df.*
    from 
      dba_data_files df
      join dba_tablespaces ts on  (df.tablespace_name = ts.tablespace_name)
      join ( select tablespace_name, file_id, sum(bytes) tot_used
             from dba_extents
             group by tablespace_name,file_id ) us on (    df.tablespace_name = us.tablespace_name 
                                                       and df.file_id = us.file_id)
--    where df.tablespace_name = 'SYSAUX'
    order by
      decode(ts.contents,'PERMANENT',1,'UNDO',2,'TEMPORARY',3,4)
      ,ts.tablespace_name
  )
select
   tablespace_name
  ,contents
  ,segment_space_management
  ,bigfile
  ,current_size_gb
  ,used_by_segments_gb
  ,current_size_gb - used_by_segments_gb                         free_space_gb
  ,((current_size_gb - used_by_segments_gb)/current_size_gb)*100 free_space_pct
  ,max_size_gb 
  ,max_size_gb - current_size_gb possible_extention_gb
  ,regexp_replace(file_name,'^([^/]*)/.*/([^/]*)$','\1/ ... /\2') file_name
from
  detail_data
/


clear columns
clear break

prompt
prompt =========================================================================
Prompt .         Actual usage of tablespaces
Prompt plus space really used by segments in each tablespace.
prompt =========================================================================
Prompt

break on report
compute sum of "TOTAL ALLOC (GB)" on report
compute sum of "TOTAL PHYS ALLOC (GB)" on report
compute sum of "USED (GB)" on report
compute sum of "FREE (GB)" on report

column "TOTAL ALLOC (GB)"       format 999G990D00
column "TOTAL PHYS ALLOC (GB)"  format 999G990D00
column "USED (GB)"              format 999G990D00
column "FREE (GB)"              format 999G990D00
column "% USED"                 format 990G00

SELECT
    a.tablespace_name,
    a.bytes_alloc / ( 1024 * 1024 * 1024)                             "TOTAL ALLOC (GB)",
    a.physical_bytes / ( 1024 * 1024 * 1024)                          "TOTAL PHYS ALLOC (GB)",
    nvl(b.tot_used, 0) / ( 1024 * 1024 * 1024)                         "USED (GB)",
    ( nvl(b.tot_used, 0) / a.bytes_alloc ) * 100                 "% USED"
FROM
    (
        SELECT
            tablespace_name,
            SUM(bytes)                                                           physical_bytes,
            SUM(decode(autoextensible, 'NO', bytes, 'YES', maxbytes))                bytes_alloc
        FROM
            dba_data_files
        GROUP BY
            tablespace_name
    )  a,
    (
        SELECT
            tablespace_name,
            SUM(bytes) tot_used
        FROM
            dba_segments
        GROUP BY
            tablespace_name
    )  b
WHERE
        a.tablespace_name = b.tablespace_name (+)
--and   (nvl(b.tot_used,0)/a.bytes_alloc)*100 > 10
    AND a.tablespace_name NOT IN (
        SELECT DISTINCT
            tablespace_name
        FROM
            dba_temp_files
    )
    AND a.tablespace_name NOT LIKE 'UNDO%'
ORDER BY
    1
--order by 5
/


clear columns
clear break

prompt
prompt =========================================================================
Prompt .         Hig Water Mark analysis and commands to reduce datafiles
Prompt Down to th High Water Mark.
prompt =========================================================================
Prompt

column current_size_gb          format 999G990D00     heading "Current Size (GB)"
column hwm_mb                   format 999G999G990D00 heading "Hwm (MB)"
column savings_gb               format 999G990D00     heading "Possible Savings (GB)"
column command                  format a200           heading "Resize Command"

break on report
compute sum of current_size_gb on report
comput sum of savings_gb on report

SELECT 
    ceil(blocks *(a.blocksize) / 1024 / 1024 / 1024)                                                                               current_size_gb,
    ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024)                                                                               hwm_mb,
    ceil(blocks *(a.blocksize) / 1024 / 1024/1024) - ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024/1024)                         savings_gb,
    'alter database datafile '''
    || file_name
    || ''' resize '
    || ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 100) * 100
    || 'm;'                                                                                                                        command
FROM
    (
        SELECT /*+PARALLEL*/
            a.*,
            p.value blocksize
        FROM
                 dba_data_files a
            JOIN v$parameter p ON p.name = 'db_block_size'
    )  a
    LEFT JOIN (
        SELECT /*+PARALLEL*/
            file_id,
            MAX(block_id + blocks - 1) hwm
        FROM
            dba_extents
        GROUP BY
            file_id
    )  b ON a.file_id = b.file_id
WHERE
    ceil(blocks *(a.blocksize) / 1024 / 1024) - ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024) > 100 /* Minimum MB it must shrink by to be considered. */
ORDER BY
    savings_gb DESC;



