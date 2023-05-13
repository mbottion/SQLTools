set markup HTML off
define p1="&1"
define p2="&2"

set feedback off
set serveroutput on
begin
  if (upper('&1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|   getAwr.sql [start] [end] 
|
|      Get AWR Report
|
|   Parameters :
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Midnight (yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
|
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/


-- -----------------------------------------------------------------
-- Parameters
-- -----------------------------------------------------------------

--
--  Analysis start date : Default (If before noon, noon yesterday, otherwise noon)
--
define start_date_FR="case when '&P1' is null then trunc(sysdate) else to_date('&P1','dd/mm/yyyy hh24:mi:ss') end"
--
--  Analysis end date : default now
--
define end_date_FR="case when '&P2' is null then sysdate else to_date('&P2','dd/mm/yyyy hh24:mi:ss') end"





set term off
set heading off
set pages 0
col dbid new_value dbid  noprint
col bsnap new_value bsnap noprint
col esnap new_value esnap noprint
col proc_name new_value proc_name noprint
col output_format new_value output_format noprint
col termState new_value termState noprint
col btime noprint
col etime noprint

select 'txt' output_format from dual where 1=2 ;
select dbid from v$database ;
select min(snap_id) bsnap ,min(begin_interval_time) btime from dba_hist_snapshot where begin_interval_time >= &start_date_FR and dbid=&dbid;
select max(snap_id) esnap ,max(begin_interval_time) etime from dba_hist_snapshot where begin_interval_time <  &end_date_FR   and dbid=&dbid;
select case when upper('&output_format') = 'HTML' then 'AWR_GLOBAL_REPORT_HTML' else 'AWR_GLOBAL_REPORT_TEXT' end proc_name from dual ;
select case when upper('&output_format') = 'HTML' then 'OFF'                    else 'ON'                     end termState from dual ;
set term &termState

select
 *
from
  table (DBMS_WORKLOAD_REPOSITORY.&proc_name (&dbid
                                             ,''
                                             ,&bsnap
                                             ,&esnap
                                             )
        )
/

