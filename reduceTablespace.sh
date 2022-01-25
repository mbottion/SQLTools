VERSION=0.1
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#   Appelé par l'option -T, permet de tester des parties de script
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
testUnit()
{
  startStep "Test de fonctionnalites"

  endStep
}
showVars()
{
  echo
  echo "==============================================================="
  echo "Variables : "
  echo "==============================================================="
  echo "  - TNS_ADMIN        : $TNS_ADMIN"
  echo
  echo "  - pDB=$pDB"
  echo "  - pPDB=$pPDB"
  echo "  - pTBS=$pTBS"
  echo "  - pMAXRUN=$pMAXRUN"
  echo "  - pPARA=$pPARA"
  echo "  - pMODE=$pMODE"
  echo "  - pNEWTS=$pNEWTS"
  echo
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#     Trace
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
startRun()
{
  START_INTERM_EPOCH=$(date +%s)
  echo   "========================================================================================" 
  echo   " Execution start"
  echo   "========================================================================================" 
  echo   "  - $1"
  echo   "  - Started at     : $(date)"
  echo   "========================================================================================" 
  echo
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#     Trace
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
endRun()
{
  END_INTERM_EPOCH=$(date +%s)
  all_secs2=$(expr $END_INTERM_EPOCH - $START_INTERM_EPOCH)
  mins2=$(expr $all_secs2 / 60)
  secs2=$(expr $all_secs2 % 60 | awk '{printf("%02d",$0)}')
  echo
  echo   "========================================================================================" 
  echo   "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
  echo   "  - Ended at      : $(date)" 
  echo   "  - Duration      : ${mins2}:${secs2}"
  echo   "========================================================================================" 
  echo   "Script LOG in : $LOG_FILE"
  echo   "========================================================================================" 
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#     Trace
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
startStep()
{
  STEP="$1"
  STEP_START_EPOCH=$(date +%s)
  echo
  echo "       - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
  echo "       - Step (start)  : $STEP"
  echo "       - Started at    : $(date)" 
  echo "       - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
  echo
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#     Trace
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
endStep()
{
  STEP_END_EPOCH=$(date +%s)
  all_secs2=$(expr $STEP_END_EPOCH - $STEP_START_EPOCH)
  mins2=$(expr $all_secs2 / 60)
  secs2=$(expr $all_secs2 % 60 | awk '{printf("%02d",$0)}')
  echo
  echo "       - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
  echo "       - Step (end)    : $STEP"
  echo "       - Ended at      : $(date)" 
  echo "       - Duration      : ${mins2}:${secs2}"
  echo "       - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#     Abort du programme
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
die() 
{
  [ "$START_INTERM_EPOCH" != "" ] && endRun
  echo "
ERROR :
  $*"

  exit 1
}

exec_sql()
{
#
#  Don't forget to use : set -o pipefail un the main program to have error managempent
#
  local VERBOSE=N
  local SILENT=N
  if [ "$1" = "-silent" ]
  then 
    SILENT=Y
    shift
  fi
  if [ "$1" = "-no_error" ]
  then
    err_mgmt="whenever sqlerror continue"
    shift
  else
    err_mgmt="whenever sqlerror exit failure"
  fi
  if [ "$1" = "-verbose" ]
  then
    VERBOSE=Y
    shift
  fi
  local login="$1"
  local stmt="$2"
  local lib="$3"
  local bloc_sql="$err_mgmt
set recsep off
set head off 
set feed off
set pages 0
set tab off
set lines 2000
connect ${login}
$stmt"
  REDIR_FILE=""
  REDIR_FILE=$(mktemp)
  if [ "$lib" != "" ] 
  then
     printf "%-75s : " "$lib";
     sqlplus -s /nolog >$REDIR_FILE 2>&1 <<%EOF%
$bloc_sql
%EOF%
    status=$?
  else
     sqlplus -s /nolog <<%EOF% | tee $REDIR_FILE  
$bloc_sql
%EOF%
    status=$?
  fi
  if [ $status -eq 0 -a "$(egrep "SP2-" $REDIR_FILE)" != "" ]
  then
    status=1
  fi
  if [ "$lib" != "" ]
  then
    [ $status -ne 0 ] && { echo "*** ERREUR ***" ; test -f $REDIR_FILE && cat $REDIR_FILE ; rm -f $REDIR_FILE ; } \
                      || { echo "OK" ; [ "$VERBOSE" = "Y" ] && test -f $REDIR_FILE && sed -e "s;^;    > ;" $REDIR_FILE ; }
  fi 
  rm -f $REDIR_FILE
  [ $status -ne 0 ] && return 1
  return $status
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#  Usage
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
usage() 
{
 echo "$SCRIPT : $SCRIPT_LIB

Usage :
 $SCRIPT -d database -p PDB [-T tablespace] [-R maxRun] [-P parallel] [-F] [-S] [-E extents]
         [-h|-?]

      Description of the script

         -d database       : Container database name         Default : None
         -p PDB Name       : Pluggable DB name               Default : None
         -T tablespace     : Tablespaces to process (regexp) Default : ALL
         -R maxRun         : maximum run time allowed        Default : 1 Hour
         -P parallel       : Degree of parallelism           Default : 8
         -F                : DO the moves OFFLINE            Default : ONLINE
         -S                : Move to same tablespace         Default : New tablespace       
         -E extents        : Process only the first extents  Delault : -1 (All)
         -?|-h        : Help

  Version : $VERSION
  "
  exit
}
analyzeSpace()
{
  RESIZE_COMMAND=""
  if [ "$1" = "AFTER" ]
  then
    RESIZE_COMMAND="\
    ,'alter database datafile '''
    || file_name
    || ''' resize '
    || ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 100) * 100
    || 'm;'      command"
  fi

  exec_sql "/ as sysdba" "

set term off
alter session set container=$pPDB ;
set term on

column current_size_gb          format 999G990D00     heading \"Current Size (GB)\"
column hwm_gb                   format 999G990D00     heading \"Hwm (GB)\"
column savings_gb               format 999G990D00     heading \"Possible Savings (GB)\"
column command                  format a200           heading \"Resize Command\"
column file_name                format a65            heading \"File\"
break on report
compute sum of current_size_gb on report
compute sum of savings_gb on report
set pages 1000
set heading on

SELECT 
    regexp_replace(file_name,'(^[^/]*)/.*/([^/]*$)','\1/ .. /\2') file_name,
    ceil(blocks *(a.blocksize) / 1024 / 1024 / 1024)                                                         current_size_gb,
    ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 1024)                                                  hwm_gb,
    ceil(blocks *(a.blocksize) / 1024 / 1024/1024) - ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024/1024)   savings_gb $RESIZE_COMMAND
FROM
    (
        SELECT /*+PARALLEL*/
            a.*,
            p.value blocksize
        FROM
                 dba_data_files a
            JOIN v\$parameter p ON p.name = 'db_block_size'
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
ORDER BY
    savings_gb DESC
/
  "
}
moveObjects()
{
  callNumber=$1
  exec_sql "/ as sysdba" "
set term off
alter session set container=$pPDB ;
set term on
REM set serveroutput on size unlimited
declare
  param_tablespace_name    varchar2(100) := '$pTBS'                                          ;  -- Tablespace Selection (regexp)
  param_stop_date          date          :=  to_date('$pSTOPDATE','dd/mm/yyyy hh24:mi:ss')   ;  -- Number of Hours to run
  param_parallel           number        :=  $pPARA                                          ;  -- Degree of parallelism
  param_online             varchar2(30)  := '$pMODE'                                         ;  -- ONLINE/OFFLINE Processing
  param_move_to_new        boolean       := '$pNEWTS' = 'Y'                                  ;  -- Move to a new tablespace
  param_max_rows           number        :=  $pEXTENTS                                       ;  -- Number of lines (for tests)
  param_real_time_log_dir  varchar2(100) := '$RT_DIR'                                        ;  -- ORACLE DIR for the LOG (Created if non existent)
  param_real_time_log_file varchar2(100) := '$RT_LOG'                                        ;  -- Name of the temporary log (Removed at the end)
  param_run_it             boolean       :=  $run_it                                         ;  -- If false, no move operation performed

  const_check_space        number        := 200                                              ;  -- Check-space every X segments

  command varchar2(1000) ;
  
  start_run  timestamp ;
  end_run    timestamp ;
  start_stmt timestamp ;
  end_stmt   timestamp ;
  
  highest_block number ;
  previous_highest_block number ;
  initial_highest_block number ;
  ora_log_dir varchar2(30) ;
  first_call boolean := true ;
  abort_loop boolean := false ;
  extent_size number ;
  free_before_hwm1 number ;
  free_before_hwm2 number ;
  free_after_hwm1 number ;
  free_after_hwm2 number ;
  segments_count number ;
  nb_errors number := 0 ;
  err number := 0 ;
  function getOnlineClause return varchar2 is
  --
  --  Returns the ONLINE/PARALLEL clause for statements
  --
  begin
    return (case param_online   when 'ONLINE' then 'ONLINE' else ''                            end  || ' ' ||
            case param_parallel when 1        then ''       else 'parallel ' || param_parallel end
           ) ;
  end ;
             
  function getTablespaceClause ( t in varchar2 ) return varchar2 is
  begin
    return (case param_move_to_new when true then 'TABLESPACE ' || t end) ;
  end ;

  function get_command(p_owner in varchar2,p_segment_name in varchar2 ,p_partition_name in varchar2
                      ,p_segment_type in varchar2,p_block_id in number ,dest_tablespace in varchar2) return varchar2 is
  --
  --  Genrerates the MOVE comment depending on the object type
  --
  tmp varchar2(2000) := '' ; 
    
  lp_table_owner        dba_lob_partitions.table_owner%type ;
  lp_table_name         dba_lob_partitions.table_name%type ;
  lp_partition_name     dba_lob_partitions.partition_name%type ;
  lp_lob_partition_name dba_lob_partitions.lob_partition_name%type ;
  lp_lob_name           dba_lob_partitions.lob_name%type ;
       
  in_partitioned        dba_indexes.partitioned%type ;
            
  ta_partitioned        dba_tables.partitioned%type ;
                  
  lo_table_name         dba_lobs.table_name%type ;
  lo_column_name        dba_lobs.column_name%type ;
                  
  begin
    if ( p_segment_type = 'LOB PARTITION')
    then
      --
      -- Get necessary informations for the segment
      --
      select table_owner    , table_name    , partition_name    , lob_partition_name    , lob_name    
      into   lp_table_owner , lp_table_name , lp_partition_name , lp_lob_partition_name , lp_lob_name 
      from   dba_lob_partitions 
      where  lob_partition_name = p_partition_name and table_owner=p_owner ;  
            
      select table_name    , COLUMN_NAME
      into   lo_table_name , lo_column_name
      from   dba_lobs where segment_name=lp_lob_name ;

      --
      -- Segment move statement generation
      --
      tmp :=  'alter table ' || lp_table_owner || '.' || lo_table_name || 
              ' move partition ' ||lp_partition_name || ' ' || 
              ' lob(' || lo_column_name || ') store as (tablespace '|| dest_tablespace || ') ' || getOnlineClause ;

    elsif ( p_segment_type = 'LOBSEGMENT')
    then
      select table_name    , COLUMN_NAME
      into   lo_table_name , lo_column_name
      from   dba_lobs where segment_name=p_segment_name ;
      
      tmp :=  'alter table ' || p_owner || '.' || lo_table_name || ' move ' ||
              ' lob(' || lo_column_name || ') store as (tablespace '|| dest_tablespace || ') ' || getOnlineClause ;

    elsif (p_segment_type = 'TABLE')
    then
      select partitioned
      into   ta_partitioned
      from   dba_tables 
      where  owner = p_owner and table_name = p_segment_name ;
 
      if ( ta_partitioned = 'YES' )
      then
        --
        --  This should not occur, if partitioned, the table has no segments
        --
        tmp := tmp || '/* partitioned table */' ;
      else
        tmp := 'alter table ' || p_owner || '.' || p_segment_name || 
               ' move ' || getTablespaceClause(dest_tablespace) || ' ' ||getOnlineClause ;
      end if ;

    elsif (p_segment_type = 'TABLE PARTITION')
    then
        
      tmp := 'alter table ' || p_owner || '.' || p_segment_name || 
             ' move partition '|| p_partition_name || ' ' || getTablespaceClause(dest_tablespace) || ' ' || getOnlineClause ;

    elsif (p_segment_type = 'TABLE SUBPARTITION')
    then

      tmp := 'alter table ' || p_owner || '.' || p_segment_name || 
             ' move subpartition '|| p_partition_name ||' ' || getTablespaceClause(dest_tablespace) || ' ' || getOnlineClause;

    elsif (p_segment_type = 'INDEX')
    then

      select partitioned
      into   in_partitioned
      from   dba_indexes 
      where  owner = p_owner and index_name = p_segment_name ;
                 
      if ( in_partitioned = 'YES' )
      then
        tmp := tmp || '/* partitioned index */' ;
      else
        tmp := 'alter index ' || p_owner || '.' || p_segment_name || 
               ' rebuild ' || getTablespaceClause(dest_tablespace) || ' ' || getOnlineClause;
      end if ;
       
    elsif (p_segment_type = 'INDEX PARTITION')
    then

      tmp := 'alter index ' || p_owner || '.' || p_segment_name || 
             ' rebuild partition '|| p_partition_name ||' ' || getTablespaceClause(dest_tablespace) || ' ' || getOnlineClause;

    elsif (p_segment_type = 'INDEX SUBPARTITION')
    then

      tmp := 'alter index ' || p_owner || '.' || p_segment_name || 
             ' rebuild subpartition '|| p_partition_name ||' ' || getTablespaceClause(dest_tablespace) || ' ' || getOnlineClause;

    end if ;

    return tmp ;

  exception 
  when others then
    --
    -- Should not occur
    --
      return tmp || ' ' || sqlerrm ;
  end ;


  procedure message(m in varchar2,indent in varchar2 default ' ',ts in boolean default true ) is
  --
  --   Print a formatted message with date
  --
    dat varchar2(30) := to_char(current_timestamp,'dd/mm/yyyy hh24:mi:ss.FF3') ;
    mess varchar2(2000) ;
    fh utl_file.file_type ;
  begin
    if not ts then dat := '.                      ' ; end if ;
    mess := dat || indent || m ;
    dbms_output.put_line( mess );
    if ( ora_log_dir is not null )
    then
      --
      --   Prints the message in a file
      --
      if first_call
      then
        fh := utl_file.fopen ( ora_log_dir,param_real_time_log_file,'w') ;
        first_call := false ;
      else
        fh := utl_file.fopen ( ora_log_dir,param_real_time_log_file,'a') ;
      end if ;
      utl_file.put_line ( fh, mess) ;
      utl_file.fclose (fh) ;
    end if ;
  end ;

  function getHighestBlock (p_tablespace_name in varchar2) return number is
  --
  -- Get the last block of a tablespace (works only for BIGFILE tablespaces)
  --
    b number ;
  begin
    begin
      select /*+ ALL_ROWS */ max(block_id) into b 
      from dba_extents where tablespace_name = p_tablespace_name ;
    exception
      when no_data_found then b := 1 ;
    end ;
    return (nvl(b,0)) ;
  end ;

  function createDir(physical_dir in varchar2) return varchar2 as
  --
  --    Create the directory, if an existing directory points to the same
  -- physical path, use it.
  --
    dn varchar2(30) ;
  begin
    message('Create real time log dir'
           ,' --+--> ');
    message(''
           ,'   |',ts=>false) ;
    begin
      select directory_name 
      into   dn
      from   dba_directories
      where  directory_path = physical_dir ;
      message('Already exists'
             ,'   +--> ',ts=>false) ;
    exception
      when no_data_found then
        dn := 'REDUCE_TS_DIR' ;
        message('Creating ' || dn
               ,'   +---+--> ',ts=>false) ;
        begin
          execute immediate 'create or replace directory ' || dn || ' as ''' || physical_dir || '''' ;
          message ('Success'
                  ,'          > ',ts=>false) ;
        exception
          when others then
            dn := null ;
            message ('*** ERROR : ' || sqlerrm
                    ,'          > ',ts=>false) ;
        end ;
    end ;
    return (dn) ;
  end ;

  --
  -- Formating functions
  --
  function fmt1(n number) return varchar2 is begin return(to_char(n,'999G999G999G999G990')) ; end ;
  function fmt2(n number) return varchar2 is begin return(to_char(n,'999G990D00')) ; end ;
  function fmt3(n number) return varchar2 is begin return(to_char(n,'999G999G990')) ; end ;

  procedure get_free_before_after(m in varchar2,i in varchar2,l in varchar2 ,ts in varchar2,blk in number ,b in out number, a in out number) as
  --
  --  Get free space size before and after a given block (BIGFILE tablespaces)
  --
  begin
    select
       b.gb_before
      ,a.gb_after
    into
       b
      ,a
    from (select /*+ PARALLEL(8) */ sum(bytes)/1024/1024/1024 gb_before
          from dba_free_space 
          where tablespace_name=ts
          and block_id < blk
         ) b
        ,(select  /*+ PARALLEL(8) */ sum(bytes)/1024/1024/1024 gb_after
          from dba_free_space 
          where tablespace_name=ts
          and block_id > blk
         ) a ;
     message(m || fmt1(b) || ' GB --> ['||l||'] <-- ' || fmt2(a) || ' GB'
            ,i,ts=>false) ;
  end ;

  function run_sql(command in varchar2) return number is
  --
  --  Run a SQL statement
  --
    errCode number ;
  begin
    message ('Running :' || command
            ,'      |  > ',ts=>false);
    message (''
            ,'      | ',ts=>false);
    start_stmt := systimestamp ;
    begin
      if ( param_run_it )
      then
        execute immediate command ;
        errCode:= sqlcode ;
        message ('Success'
                ,'      +--> ',ts=>false);
      else
        message ('Test mode (command not ran)'
                ,'      +--> ',ts=>false);
        errCode := 0 ;
      end if ;
    exception
    when others then
      errCode := sqlcode ;
      if ( sqlcode = -2149 )
      then
        message ('Object already moved (-2149)'
                ,'      +--> ',ts=>false);
      elsif ( sqlcode = -2327 )
      then
        message ('LOB Index, not rebuilt (-2327)'
                ,'      +--> ',ts=>false);
      else
        nb_errors := nb_errors + 1 ;
        message ('*** ERROR **  : ' || sqlerrm
                ,'      +--> ',ts=>false);
      end if ;
    end ;
    end_stmt := systimestamp ;
    message (''
            ,'         > ',ts=>false);
    message ('Time          : ' || (end_stmt -start_stmt)
            ,'         > ',ts=>false);
    return(errCode) ;
  end ;

  procedure abort_message(m in varchar2) is
  --
  --  Print a visible abord message
  --
  begin
    message ('',ts=>false);
    message (rpad('*',90,'*'),ts=>false);
    message('*** ' || rpad(m,80) || '  ****' ) ;
    message (rpad('*',90,'*'),ts=>false);
    message ('',ts=>false);
  end ;

  procedure checkOrCreateDestTS (old_ts in varchar2 ) is
  -- If the new TS does not exists, create it and alter user quotas
    new_ts varchar2(100) := old_ts || '_NEW' ;
    dummy number ;
    err number ;
  begin
    message ('','',false) ;
    message ('- Move to new = Yes, moving from ' || old_ts || ' to ' || new_ts ,'',false) ;
    message ('  --------------------------------------------------------','',false) ;
    message ('','',false) ;
    message('Check existence of ' || new_ts || '...' 
              ,' --+--> ');
    begin
      select  1
      into    dummy
      from    dba_tablespaces
      where   tablespace_name = new_ts ;
      message ('Exists'
              ,'   |  > ',ts=>false);
    exception
      when no_data_found then
        message ('Non existing TABLESPACE'
                ,'   |  > ',ts=>false);
        message (''
                ,'   |  > ',ts=>false);
        message ('Creating tablespace ' || new_ts 
                ,'   +--+--> ',ts=>false);
        err := run_sql('create bigfile tablespace ' || new_ts ) ;
        message (''
                ,'   |  > ',ts=>false);
        message ('Set quotas'
                ,'   +--+--> ',ts=>false);
        for recQuotas in (select 'alter user ' || username || ' quota ' || 
                                 case when MAX_BYTES = -1 then 'UNLIMITED' else to_char(MAX_BYTES) end || 
                                 ' ON ' || new_ts command
                          from   dba_ts_quotas
                          where  tablespace_name = old_ts)
        loop
          err := run_sql(recQuotas.command) ;
        end loop ;
        for recDefTS in (select 'alter user ' || username || ' default tablespace ' || new_ts command
                         from   dba_users where default_tablespace = old_ts) 
        loop
          err := run_sql(recDefTS.command) ;
        end loop ;
    end ;
  end ;
  
-- -----------------------------------------------------------------------
--                       M A I N   P R O G R A M 
-- -----------------------------------------------------------------------

begin
  if param_max_rows = -1 
  then
    --
    --    All rows (nearly ;-)
    --
    param_max_rows := 100000000 ;
  end if ;

  --
  -- Create the directory or get the name of an existing one
  --
  ora_log_dir := createDir(param_real_time_log_dir) ;
  message('','',ts=>false);

  start_run := systimestamp ;
  
  --
  -- Prints the parameters of this run
  --
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|          Move upper extents down in the tablespace'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  Tablespace Selection  : ' || param_tablespace_name
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Stop Date             : ' || to_char(param_stop_date,'dd/mm/yyyy hh24:mi:ss') 
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Max run duration      : ' || to_char((param_stop_date-cast(start_run as date))*24,'990D99') || ' Hours'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Online/Offline        : ' || param_online
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Parallel              : ' || param_parallel
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Move to new TS        : ' || case param_move_to_new when true then 'Yes' else 'No' end
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Maximun Extents       : ' || param_max_rows
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Start Date            : ' || to_char(start_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);

  dbms_application_info.set_client_info('Move Tablespaces ($SCRIPT)') ;
  --
  --     Loop on the tablespaces depending on the criteria given (regexp format)
  --
  for tablespaces in ( select t.tablespace_name 
                             ,t.block_size
                             ,f.file_name
                       from   dba_tablespaces t
                       join   dba_data_files f on (t.tablespace_name = f.tablespace_name )
                       where  regexp_like (t.tablespace_name,param_tablespace_name )
                       and    t.tablespace_name not in ('SYSTEM','SYSAUX') 
                       and    t.contents = 'PERMANENT' 
                       and    t.tablespace_name not like '%NEW'
                       and    t.bigfile = 'YES' /* This script has only been tested with BIGFILE tablespaces */
                       --and    1=2
                     )
  loop

    --
    --  Print tablespace informations and starting block
    --
    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    message (rpad('|  Tablespace            : ' || tablespaces.tablespace_name
                           ,80,' ') || '|','    ',ts=>false) ;
    highest_block := getHighestBlock(tablespaces.tablespace_name) ;
    previous_highest_block := highest_block ;
    initial_highest_block := highest_block ;
    message (rpad('|  Initial highest block : ' || fmt1(initial_highest_block)
                           ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    dbms_application_info.set_module('TBS: ' || tablespaces.tablespace_name,'Start') ;

    if param_move_to_new 
    then
      checkOrCreateDestTS (tablespaces.tablespace_name) ;
    end if ;
    if ( param_move_to_new )
    then
      message ('','',false) ;
      message ('- Allocate extent to empty tables ...','',false) ;
      message ('  -----------------------------------','',false) ;
      message ('','',false) ;
      dbms_application_info.set_action('Allocating empty segments') ;

      --for recUnallocatedIndexes in (select 'begin  dbms_space_admin.materialize_deferred_segments(''' || table_owner || ''',''' || table_name || ''') ; end ; ' command
      for recUnallocatedTables in  (select   'alter table ' || owner || '.' || table_name || ' allocate extent ' command
                                    from     dba_tables t
                                    where    tablespace_name = tablespaces.tablespace_name
                                    and      not exists (select 1 
                                                         from   dba_segments s
                                                         where  s.owner = t.owner 
                                                         and    s.segment_name = t.table_name) 
                                    UNION
                                    select   'alter table ' || table_owner || '.' || table_name || 
                                             ' modify partition ' || partition_name || ' allocate extent ' command
                                    from     dba_tab_partitions t
                                    where    tablespace_name = tablespaces.tablespace_name
                                    and      not exists (select 1
                                                         from   dba_segments s
                                                         where  s.owner = t.table_owner
                                                         and    s.segment_name = t.table_name
                                                         and    s.partition_name = t.partition_name) 
                                   )
      loop
        err := run_sql (recUnallocatedTables.command) ;
      end loop ;
    end if ;

    message ('','',false) ;
    message ('- Moving segments ...','',false) ;
    message ('  -------------------','',false) ;
    message ('','',false) ;

    --
    --    Select segments by their last block-id, the aim is to try to always move
    -- the segment containing the highest block. If there is sufficient free space 
    -- before the HWM, the segment will have its blocks moved down. Otherwise
    -- the segment will be moved up (and the loop will exit, wasting a little 
    -- space.
    --
    segments_count := 0 ;
    for extentsToMove in (
              with 
              high_segs as (
                                select /*+ ALL_ROWS */
                                   owner
                                  ,segment_name
                                  ,partition_name
                                  ,segment_type
                                  ,max(block_id) block_id
                                from
                                   dba_extents
                                where
                                  tablespace_name = tablespaces.tablespace_name
                                group by
                                   owner
                                  ,segment_name
                                  ,partition_name
                                  ,segment_type
                                order by block_id desc
                                fetch first param_max_rows rows only )
              select 
                   owner
                  ,segment_name
                  ,partition_name
                  ,segment_type
                  ,block_id
              from 
                high_segs
              order by block_id desc
         )
    loop
      if (systimestamp > param_stop_date)
      then
        --
        --     If the script run for more than the allotted time, then
        -- we abort. 
        --
        --     Attention, if we run OFFLINE, there will still remain some
        -- time spent in the UNUSABLE indexes rebuild. When running ONLINE
        -- the indexes should have been maintained during the move operations.
        --
        abort_message('Aborting the script execution after ' || to_char(param_stop_date,'dd/mm/yyyy hh24:mi:ss')) ;
        abort_loop := true ;
        exit ;
      end if ;
      segments_count := segments_count + 1 ;
      dbms_application_info.set_action('Moving: ' || extentsToMove.owner ||'.'|| extentsToMove.segment_name || ' (' || extentsToMove.partition_name || ')') ;
      
      message('Segment : ' || rpad(nvl(extentsToMove.owner ||'.'|| extentsToMove.segment_name ,' '),50) || rpad('('||nvl(extentsToMOve.segment_type,' ')||')',30) || '(# : ' || fmt3(segments_count) || ')' 
              ,' --+--> ');
            
      command := get_command(extentsToMove.owner,extentsToMove.segment_name,extentsToMove.partition_name
                            ,extentsToMove.segment_type,extentsToMove.block_id
                            ,tablespaces.tablespace_name || case when param_move_to_new then '_NEW' else '' end);

      if (command is not null)
      then
        message (''
                ,'   | ',ts=>false);
        message ('Block : ' || fmt1(extentsToMove.block_id) 
                ,'   +--+--> ',ts=>false);
        message(''
                ,'      |  > ',ts=>false);

        if ( not param_move_to_new or mod(segments_count,const_check_space) = 0)
        then
          --
          --   Calculation HWM is very long, not really necessary at each segment 
          -- when movin to a new tablespace
          --
          get_free_before_after('Before move   : '
                               ,'      |  > '
                               ,extentsToMove.segment_name
                               ,tablespaces.tablespace_name
                               ,extentsToMove.block_id
                               ,free_before_hwm1
                               ,free_after_hwm1) ;
        end if ;


        --
        --   Run the statement, errors in the move operation are simply printed, they
        -- do not stop the execution
        --
        err := run_sql (command) ;

        if (err = -2327)
        then
          if ( extentsToMove.partition_name is not null )
          then
            message('Move the corresponding LOB Partition'
                   ,' --+--> ');
            declare 
              new_ts varchar2(100) ;
            begin
              new_ts := tablespaces.tablespace_name || case when param_move_to_new then '_NEW' else '' end ;
              select
                'alter table ' || table_owner ||'.' ||table_name || ' move partition ' || partition_name ||
                ' LOB (' || column_name || ') store as (tablespace '|| new_ts || ') ' 
              into
                command
              from
                dba_lob_partitions 
              where (table_owner,table_name,lob_indpart_name) = (   
                                                                 SELECT
                                                                       i.table_owner
                                                                      ,i.table_name
                                                                      ,ip.partition_name
              --                                                        ,ip.*
                                                                  FROM
                                                                      dba_ind_partitions ip
                                                                  JOIN dba_indexes i on (i.owner = ip.index_owner
                                                                                         and i.index_name = ip.index_name )
                                                                  WHERE
                                                                          ip.index_name = extentsToMove.segment_name
                                                                      AND ip.partition_name = extentsToMove.partition_name
                                                                      AND ip.index_owner = extentsToMove.owner
                                                                      ) ;
              command := command || getOnlineClause ;
              err := run_sql (command) ;
            exception
              when no_data_found then
                message('Object not found'
                       ,'      |  > ',ts=>false);
            end ;
          end if ;
        end if ;
        if ( err in (-1450,-14808) )
        then
          nb_errors := nb_errors - 1 ;
          message('Run the command Again OFFLINE mode'
                  ,'      |  > ',ts=>false);
          err := run_sql(replace (command,' ONLINE ',' ')) ;
        end if ;

        message ('Errors so far : ' || nb_errors 
                ,'         > ',ts=>false);
     
        if ( nb_errors > 200 )
        then
          abort_message('Too many errors were found, aborting the script' ) ;
          abort_loop := true ;
          exit ;
        end if ;

        if ( not param_move_to_new or (mod(segments_count,const_check_space) = 0 and free_after_hwm1 > 500))
        then
          --
          --   Calculation HWM is very long, so when moving to new TS, we do not
          -- need to get it at each segment.
          --

          --
          --  Print new information about blocks and HWM
          --
          dbms_application_info.set_action('Calculate HWM') ;
          highest_block := getHighestBlock(tablespaces.tablespace_name) ;
          message ('Highest Block : ' || fmt1(highest_block) || ' variation (' || (highest_block-previous_highest_block) || ')'
                  ,'         > ',ts=>false);

          get_free_before_after('After move    : '
                               ,'         > '
                               ,'HWM'
                               ,tablespaces.tablespace_name
                               ,highest_block
                               ,free_before_hwm2
                               ,free_after_hwm2
                               ) ;
          --
          -- This is an indication of the gain.
          --
          message ('Variation     : ' || fmt2(free_after_hwm1 - free_after_hwm2) || ' GB'
                  ,'         > ',ts=>false);
        end if ;

        if (not param_move_to_new and ((highest_block-previous_highest_block) > 0))
        then
          --
          --    if the segment has been moved up, then we stop here for this tablespace
          --
          abort_message('Aborting the script execution because used space doesn''t decrease anymore' ) ;
          exit ;
        end if ;
        previous_highest_block := highest_block ;

       if (     param_move_to_new 
            and ( free_after_hwm2 > 500 ) 
          )
       then
         --
         -- There is more than 500 Gb free after HWM, resize the datafile
         --
         dbms_application_info.set_action('Resize to HWM') ;
         declare
           blocks_last_extent number ;
         begin
           select  blocks
           into    blocks_last_extent
           from    dba_extents
           where   tablespace_name = tablespaces.tablespace_name and block_id = highest_block ;
           message('','',false) ;
           message('Resize datafile to ' || fmt2(((highest_block+blocks_last_extent+1)*tablespaces.block_size)/1024/1024/1024) || 'GB'
                  ,' --+--> ');
           message(tablespaces.file_name
                  ,'   |  > ');
           err := run_sql ('alter database datafile '''|| tablespaces.file_name || 
                    ''' resize ' || to_char(ceil(((highest_block+blocks_last_extent+1)*tablespaces.block_size)/1024/1024/1024/100)*100) || ' G') ; 
           message('','',false) ;
         end ;
         free_after_hwm2 := 0 ;
       end if ;
      else
        --
        -- no command
        --     
        message (' *** Unable to process ' || extentsToMOve.segment_type
                ,'   +--+--> ',ts=>false);
      end if ;
        message (''
                ,'',ts=>false);
    end loop ; -- Segment LOOP



    if ( param_move_to_new and not abort_loop)
    then
      message ('','',false) ;
      message ('- Move materialized views ...','',true) ;
      message ('  ---------------------------','',false) ;
      message ('','',false) ;

      dbms_application_info.set_action('Move materialized views') ;

      for recMV in ( select 'alter materialized view ' || m.owner || '.' || m.mview_name || 
                            ' move tablespace ' || tablespaces.tablespace_name || '_NEW' command  
                     from   dba_mviews m
                     join   dba_tables t on (t.table_name = m.container_name and t.owner = m.owner )
                     where   t.tablespace_name = tablespaces.tablespace_name )
      loop
        err := run_sql(recMV.command) ;
      end loop ;

      message ('','',false) ;
      message ('- Move tables and partitions without segments ...','',true) ;
      message ('  -----------------------------------------------','',false) ;
      message ('','',false) ;

      dbms_application_info.set_action('Move tables and partitions without segments') ;

      for tNoSegs in (select 'alter table ' || t.owner || '.' || table_name || ' move tablespace ' || t.tablespace_name || '_NEW online ' command
                      from dba_tables t where     not exists (select 1 
                                                              from dba_segments s
                                                              where t.owner=s.owner 
                                                              and t.table_name = s.segment_name
                                                              and s.segment_type = 'TABLE')
                                              and tablespace_name = tablespaces.tablespace_name
                                              and partitioned = 'NO' 
                      UNION
                      select 'alter table ' || t.table_owner || '.' || table_name || ' move partition ' || t.partition_name || ' tablespace ' || t.tablespace_name || '_NEW online ' command
                      from dba_tab_partitions t where not exists (select 1 
                                                                  from dba_segments s
                                                                  where t.table_owner=s.owner 
                                                                  and t.table_name = s.segment_name
                                                                  and t.partition_name = s.partition_name
                                                                  and s.segment_type = 'TABLE PARTITION')
                                                  and tablespace_name = tablespaces.tablespace_name 
                                                  and composite = 'NO'
                     UNION
                     select 'alter table ' || lp.table_owner || '.' || lp.table_name || ' move partition ' || lp.partition_name ||
                            ' lob ( ' || lp.column_name || ') store as ( tablespace ' || lp.tablespace_name || '_NEW ) online ' command
                     from dba_lob_partitions lp where not exists (select 1
                                                                  from dba_segments s
                                                                  where lp.table_owner = s.owner 
                                                                  and   lp.lob_name = s.segment_name
                                                                  and   s.tablespace_name = lp.tablespace_name)
                                                  and tablespace_name = tablespaces.tablespace_name
                    )
      loop
        err := run_sql (tNoSegs.command) ;
      end loop ;

      message ('','',false) ;
      message ('- Change composite partitions attributes ...','',true) ;
      message ('  ------------------------------------------','',false) ;
      message ('','',false) ;
      dbms_application_info.set_action('Change composite partitions attributes') ;

      for recComp in (select 'alter table ' || t.table_owner || '.' || table_name || 
                             ' modify default attributes for partition ' || t.partition_name || 
                             ' tablespace ' || t.tablespace_name || '_NEW' command
                      from dba_tab_partitions t where not exists (select 1
                                                                  from dba_segments s
                                                                  where t.table_owner=s.owner
                                                                  and t.table_name = s.segment_name
                                                                  and t.partition_name = s.partition_name
                                                                  and s.segment_type = 'TABLE PARTITION')
                                                and tablespace_name = tablespaces.tablespace_name
                                                and composite = 'YES' 
                      UNION
                      select 'alter table ' || l.owner || '.' || l.table_name || ' modify default attributes ' ||
                             ' lob ( ' || l.column_name || ') ( tablespace ' || l.tablespace_name || '_NEW) ' command
                             from dba_lobs l where not exists (select 1
                                                               from dba_segments s
                                                               where l.owner = s.owner
                                                               and   l.segment_name = s.segment_name
                                                               and   l.tablespace_name = s.tablespace_name)
                                             and tablespace_name = tablespaces.tablespace_name)
      loop
        err := run_sql(recComp.command) ;
      end loop ;
    end if ; 
    --
    --     If we run OFFLINE, the indexes needs to be rebuilt, we do this on a 
    --  Tablespace basis. We could have put this after each move, but with the
    --  risk of rebuilding the same index several times.
    -- 
    message ('','',false) ;
    message ('- Rebuilding UNUSABLE Indexes ...','',true) ;
    message ('  -------------------------------','',false) ;
    message ('','',false) ;
    dbms_application_info.set_action('Rebuilding UNUSABLE Indexes') ;

    for rebuildCmds in ( select owner
                               ,index_name
                               ,'' partition_name 
                               ,'alter index ' || owner || '.' || index_name || ' rebuild' command 
                         from dba_indexes 
                         where status='UNUSABLE' 
                         and   owner in (select username from dba_users where oracle_maintained = 'N')
                         and   tablespace_name = tablespaces.tablespace_name
                         UNION
                         select index_owner
                               ,index_name,partition_name
                               ,'alter index ' || index_owner || '.' || index_name || ' rebuild partition ' || partition_name 
                         from dba_ind_partitions 
                         where status='UNUSABLE' 
                         and   index_owner in (select username from dba_users where oracle_maintained = 'N')
                         and   tablespace_name = tablespaces.tablespace_name
                       )
    loop
      command := rebuildCmds.command || ' ' || getOnlineClause ;
      message('Rebuild : ' || rebuildCmds.owner || '.' || rebuildCmds.index_name || 
              case when rebuildCmds.partition_name is null then '' else '(Part : ' || rebuildCmds.partition_name || ')' end 
              ,' --+--> ');
      err := run_sql(command) ;
    end loop ;

    if ( not abort_loop )
    then
      message ('','',false) ;
      message ('- Final RESIZE ...','',true) ;
      message ('  ----------------','',false) ;
      message ('','',false) ;
      dbms_application_info.set_action('Final resize') ;
      --
      --    Print information on the tablespace (the new HWN should be lower than the initial one)
      --
      highest_block := getHighestBlock(tablespaces.tablespace_name) ;
      if (     param_move_to_new )
      then
        --
        -- resize the datafile
        --
        declare
          blocks_last_extent number ;
        begin
          begin
            select  blocks
            into    blocks_last_extent
            from    dba_extents
            where   tablespace_name = tablespaces.tablespace_name and block_id = highest_block ;
          exception
            when no_data_found then blocks_last_extent := 1 ;
          end ;
          if ( (((highest_block+blocks_last_extent+1)*tablespaces.block_size)/1024/1024/1024) > 100 )
          then
            message('','',false) ;
            message('Resize datafile to ' || fmt2(((highest_block+blocks_last_extent+1)*tablespaces.block_size)/1024/1024/1024) || 'GB'
                   ,' --+--> ');
            message(tablespaces.file_name
                   ,'   |  > ');
            err := run_sql ('alter database datafile '''|| tablespaces.file_name || 
                     ''' resize ' || to_char(ceil(((highest_block+blocks_last_extent+1)*tablespaces.block_size)/1024/1024/1024/100)*100) || ' G') ; 
            message('','',false) ;
          end if ;
        end ;
      end if ;
      message (rpad('+',80,'-') || '+','    ',ts=>false) ;
      message (rpad('|  Initial highest block : ' || fmt1(initial_highest_block)
                    ,80,' ') || '|','    ',ts=>false) ;
      message (rpad('|  Current highest block : ' || fmt1(highest_block)
                   ,80,' ') || '|','    ',ts=>false) ;
      message (rpad('|  Variation             : ' || fmt1(highest_block-initial_highest_block) || ' Blocks'
                   ,80,' ') || '|','    ',ts=>false) ;
      message (rpad('+',80,'-') || '+','    ',ts=>false) ;
      message('',ts=>false) ;
    end if ;
    --
    --    If time is up, the abort the loop
    --
    if ( abort_loop )
    then
      exit ;
    end if;
  end loop ; -- tablespaces LOOP

  --
  --     Print summary information
  --
  end_run := systimestamp ;
  
  message ('',ts=>false);
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  End of run            : ' || to_char(end_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Run duration          : ' || (end_run - start_run)
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);

end ; -- PROGRAM
/
  "
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set -o pipefail

SCRIPT=reduceTablespace.sh
SCRIPT_LIB="Move segments to reduce space usage"

[ "$(id -un)" != "oracle" ] && die "Script must be run by the \"oracle\" user"
#[ "$(hostname -s | sed -e "s;.*\([0-9]\)$;\1;")" != "1" ] && die "Lancer ce script depuis le premier noeud du cluster"

#[ "$1" = "" ] && usage

toShift=0
while getopts d:p:T:R:P:FSE:h opt
do
  case $opt in
   # --------- Database ---------------------------------------
   d)  pDB=$OPTARG       ; toShift=$(($toShift + 2)) ;;
   p)  pPDB=$OPTARG      ; toShift=$(($toShift + 2)) ;;
   # --------- Modes de fonctionnement ------------------------
   T)  pTBS=$OPTARG      ; toShift=$(($toShift + 2)) ;;
   R)  pMAXRUN=$OPTARG   ; toShift=$(($toShift + 2)) ;;
   P)  pPARA=$OPTARG     ; toShift=$(($toShift + 2)) ;;
   F)  pMODE=OFFLINE     ; toShift=$(($toShift + 1)) ;;
   S)  pNEWTS=N          ; toShift=$(($toShift + 1)) ;;
   E)  pEXTENTS=$OPTARG  ; toShift=$(($toShift + 2)) ;;
   # --------- Usage ------------------------------------------
   ?|h) usage "Help requested";;
  esac
done
shift $toShift 
# -----------------------------------------------------------------------------
#
#       Analyse des paramètres et valeurs par défaut
#
# -----------------------------------------------------------------------------

LOG_FILE=/dev/null

pTBS=${pTBS:-.\*}
pMAXRUN=${pMAXRUN:-1}
pPARA=${pPARA:-8}
pMODE=${pMODE:-ONLINE}
pNEWTS=${pNEWTS:-Y}
pEXTENTS=${pEXTENTS:-1000000}

[ "$pDB" = "" ] && pDB=$dbUniqueName # (if ran by runScript)
[ "$pPDB" = "" ] && pPDB=$pdbName # (if ran by runScript)

[ "$pDB" = "" ] && die "Name of database is mandatory"
[ "$pPDB" = "" ] && die "Name of pluggable database is mandatory"

spaceAnalysisBefore=Y
spaceAnalysisAfter=Y
run_it=true

RT_DIR='/tmp'
RT_LOG="reduceTablespace_$(date +%Y%m%d_%H%M%S).log"

pMAXRUN=$(echo $pMAXRUN | sed -e "s;\,;.;")
{
  startRun "Shell Template"

  showVars

  echo "  -Set $pDB environment"
  . $HOME/$pDB.env || die "Unable to set environment for $pDB"
  echo "  -Check $pPDB"
  res=$(exec_sql "/ as sysdba"  "select 1 from v\$pdbs where name=upper('$pPDB');") || die "Erreur select PDB cible ($res)"
  printf "%-75s : " "  - PDB check ($pPDB)"
  [ "$res" = "" ] && { echo "Not exists" ; die "Non Existing PDB $($pPDB)" ; }  \
                  || { echo "Exists" ; }

  if [ "$spaceAnalysisBefore" = "Y" ]
  then
    startStep "Analyze space before move"
    analyzeSpace BEFORE
    endStep
  fi

  dateNow=$(date +%s)
  dateEnd=$(bc -q <<%% | cut -f1 -d "."
$dateNow + ($pMAXRUN * 3600)
%%
)
  pSTOPDATE=$(date --date "@$dateEnd" "+%d/%m/%Y %H:%M:%S")

  echo "Script will automatically exit after $pMAXRUN hours ($pSTOPDATE)"
  echo

  startStep "Move segments until $pSTOPDATE"

  echo
  echo =====================================================================
  echo     During the execution of moves, nothing is printed here
  echo   you can see progress in $RT_DIR/$RT_LOG
  echo
  echo     The file is removed at the end of the execution
  echo =====================================================================
  echo

  SQL_FINISHED=N
  i=1
  while [ "$SQL_FINISHED" = "N" ]
  do
    echo "    - Start move (loop #$i)"
    res=$(moveObjects $i)
    if [ $? -ne 0 ]
    then
      SQL_FINISHED=KO
    else
      SQL_FINISHED=OK
    fi
    if [ "$SQL_FINISHED" = "KO" ]
    then
      SQL_FINISHED=N
      [ $(date +%s) -gt $dateEnd ] && SQL_FINISHED="TIME" 
      [ $i -ge 10 ] && SQL_FINISHED="LOOP" 
    fi
    i=$(($i + 1))
    [ -f $RT_DIR/$RT_LOG ] && cat $RT_DIR/$RT_LOG
    rm -f $RT_DIR/$RT_LOG
    echo "$res"
  done

  echo
  if [ "$SQL_FINISHED" = "OK" ]
  then
    echo "SQL Script finished without error"
  elif [ "$SQL_FINISHED" = "KO" ]
  then
    echo "SQL Script finished with error"
  elif [ "$SQL_FINISHED" = "LOOP" ]
  then
    echo "SQL Script finished with error $(($i - 1)) times, aborting the loop"
  else
    echo "Time's up!!!"
  fi

  endStep

  if [ "$spaceAnalysisAfter" = "Y" ]
  then
    startStep "Analyze space After move"
    analyzeSpace AFTER
    endStep
  fi
  endRun
} 2>&1 | tee $LOG_FILE