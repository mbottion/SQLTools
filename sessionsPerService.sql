set serveroutput on
begin
  if (upper('&1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    sessionsPerService.sql [start] [end]    
|   
|   Lists the sessions connected to services on a given period
|
|   Parameters :
|       start  : Analysis start date (dd/mm/yyyy [hh24:mi:ss]) - Default : Midnight today
|       end    : Analysis end date   (dd/mm/yyyy [hh24:mi:ss]) - Default : now
|       
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/

define start_time="case when '&1' is null then trunc(sysdate) else to_date('&1','dd/mm/yyyy hh24:mi:ss') end"
define   end_time="case when '&2' is null then sysdate        else to_date('&2','dd/mm/yyyy hh24:mi:ss') end"

define excluded_users_list="'SYS','SYSTEM','SYSRAC','DBSNMP','ACCENTURE','RDACCENTURE','C##MBOORA'"

column username        format a15 trunc
column machine         format a40 trunc
column program         format a30 trunc
column service_name    format a20 trunc
column action          format a20 trunc
column module          format a25 trunc

column instance_number format 999 heading inst


-- -----------------------------------------------------------------
-- SQL
-- -----------------------------------------------------------------

Prompt
Prompt      Historical Sessions
Prompt      =========================================
Prompt
select distinct
   usr.username
  ,ash.instance_number
  ,srv.service_name
  ,ash.machine
--  ,ash.user_id
--  ,ash.service_hash
  ,ash.program
  ,ash.module
  ,regexp_replace(
     regexp_replace(
       regexp_replace(
          regexp_replace(ash.ACTION
                        ,'_MV[0-9]*_(LIQ|TEC|SYN1|ACE1)[F12]*_','_')
                     ,'_[0-9]*_JOB_TASKNUM[_0-9]*$','')
                   ,'[0-9_]*$','')
                 ,'(REFRESH_MV|PR_MERGE)_.*','\1') ACTION
from
  dba_hist_ACTIVE_SESS_HISTORY  ash
  left outer join dba_users  usr on ( ash.user_id = usr.user_id )
  left join dba_hist_service_name srv on ( ash.dbid = srv.dbid and ash.service_hash = srv.service_name_hash)
where 
  sample_time between &start_time and &end_time
  and usr.username not in (&excluded_users_list)
order by
  username,service_name,machine
/

 
Prompt
Prompt      Current Sessions
Prompt      =========================================
Prompt

break on inst_id on service_name on username 
select distinct
   sess.inst_id
  ,sess.service_name
  ,sess.username
  ,sess.machine
  ,sess.module
  ,regexp_replace(
     regexp_replace(
       regexp_replace(
          regexp_replace(sess.ACTION
                        ,'_MV[0-9]*_(LIQ|TEC|SYN1|ACE1)[F12]*_','_')
                     ,'_[0-9]*_JOB_TASKNUM[_0-9]*$','')
                   ,'[0-9_]*$','')
                 ,'(REFRESH_MV|PR_MERGE)_.*','\1') ACTION
  ,to_char(sess.logon_time,'dd/mm/yyyy hh24:mi:ss') "Logon"
from
  gv$session  sess
where 
      sess.username not in (&excluded_users_list)
  and sess.username not like '%ACCENTURE'
order by
  inst_id,service_name,username,machine
/

