define P1="&1"
define P2="&2"
define P3="&3"
define P4="&4"
define P5="&5"
define P6="&6"
define P7="&7"
define P8="&8"
define P9="&9"
set serveroutput on
declare 
  n number ;
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    dbms_output.put_line('
+---------------------------------------------------------------------------------------
| Usage:
|    reduceTablespace.sql [TSName] [MaxRunHours] [parallel] [online] [extents]
|   
|         after showing the datafiles uages and highwater mark, this script will  
|   move segments having the highest block_id to save space.
|   
|         The script will stop processing segments of a tablespace when a move operation
|   has moved a segment to a higher block than the initial one, this is a little 
|   space waste, but calculating the segment''s space is too time consuming.
|
|         The operation can be done ONLINE or OFFLINE. If done online, it id very 
|   long for large segments since it cannot be run in parallel.
|
|         The processing stops after the given time limit is reached. If a segment move
|   is in progress, its move continues until the end. 
|
|         at the end of the loop for each tablespace unusable indexes are rebuilt (online)
|   the rebuild time is not included in the run time limit. 
|
|         When the run is terminated, we, again show the datafile USAGE, along with commands
|   to use to resize the datafiles.
|
|         Since the process is a PL/SQL one, the LOG is only visible at the and, so, a 
|   a temporary log file is produced by the database to see what''s gonin on. The file
|   is removed when the PL/SQL block ends
|
|   Parameters :
|       TSName          : Tablespace Name (if null, all non sys tablespaces)     - Default : NULL
|       MaxRunHours     : delay after which the script will stop moving segments - Default : 1
|       Parallel        : Degree of parallelism (for offline moves)              - Default : 32
|       online          : ONLINE | OFFLINE                                       - Default : OFFLINE
|       extents         : Number of move operations per tablespace               - Default : 500
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
define      TSName="case when '&P1' is null then '.*'      else upper('&P1')     end"
define MaxRunHours="case when '&P2' is null then 1         else to_number('&P2') end"
define    Parallel="case when '&P3' is null then 32        else to_number('&P3') end"
define      Online="case when '&P4' is null then 'OFFLINE' else upper('&P4')     end"
define     extents="case when '&P5' is null then 500       else to_number('&P5') end"

define RT_DIR='/tmp'
set term off
column RT_LOG new_value RT_LOG
select 
  'reduceTablespace_' || to_char(sysdate,'yyyymmdd_hh24miss') || '.log' RT_LOG
from
  dual
/
set term on

prompt
prompt =====================================================================
prompt .   During the execution of moves, nothing is printed here
prompt . you can see progress in &RT_DIR/&RT_LOG
prompt
prompt .   The file is removed at the end of the execution
prompt =====================================================================
prompt

prompt
prompt =========================================================================
Prompt .         High Water Mark analysis (Before run) 
prompt =========================================================================
Prompt

column current_size_gb          format 999G990D00     heading "Current Size (GB)"
column hwm_gb                   format 999G990D00     heading "Hwm (GB)"
column savings_gb               format 999G990D00     heading "Possible Savings (GB)"
column command                  format a200           heading "Resize Command"
column file_name                format a100           heading "File"
break on report
compute sum of current_size_gb on report
comput sum of savings_gb on report

SELECT 
    file_name,
    ceil(blocks *(a.blocksize) / 1024 / 1024 / 1024)                                                                               current_size_gb,
    ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 1024)                                                                        hwm_gb,
    ceil(blocks *(a.blocksize) / 1024 / 1024/1024) - ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024/1024)                         savings_gb
--    'alter database datafile '''
--    || file_name
--    || ''' resize '
--    || ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 100) * 100
--    || 'm;'                                                                                                                        command
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
--WHERE
--    ceil(blocks *(a.blocksize) / 1024 / 1024) - ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024) > 100 /* Minimum MB it must shrink by to be considered. */
ORDER BY
    savings_gb DESC
/

Rem
Rem If queries against dictionary becomes slow:
rem exec dbms_stats.gather_dictionary_stats ;
rem exec dbms_stats.gather_fixed_objects_stats ;
rem 


set serveroutput on 
declare
  param_tablespace_name    varchar2(100) := &TSName      ;  -- Tablespace Selection (regexp)
  param_max_run_time       number        := &MaxRunHours ;  -- Number of Hours to run
  param_parallel           number        := &Parallel    ;  -- Degree of parallelism
  param_online             varchar2(30)  := &Online      ;  -- ONLINE/OFFLINE Processing
  param_max_rows           number        := &extents     ;  -- Number of lines (for tests)
  param_real_time_log_dir  varchar2(100) := '&RT_DIR'    ;  -- ORACLE DIR for the LOG (Created if non existent)
  param_real_time_log_file varchar2(100) := '&RT_LOG'    ;  -- Name of the temporary log (Removed at the end)
  param_run_it             boolean       := true         ;  -- If false, no move operation performed

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

  function getOnlineClause return varchar2 is
  --
  --  Returns the ONLINE/PARALLEL clause for statements
  --
  begin
    return (case param_online   when 'ONLINE' then 'ONLINE' else ''                            end  || ' ' ||
            case param_parallel when 1        then ''       else 'parallel ' || param_parallel end
           ) ;
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
      tmp :=  'alter table ' || lp_table_owner || '.' || lo_table_name || ' move partition ' ||lp_partition_name ||
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
        tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move ' || getOnlineClause ;
      end if ;

    elsif (p_segment_type = 'TABLE PARTITION')
    then
        
      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move partition '|| p_partition_name ||' ' || getOnlineClause ;

    elsif (p_segment_type = 'TABLE SUBPARTITION')
    then

      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move subpartition '|| p_partition_name ||' ' || getOnlineClause;

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
        tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild ' || getOnlineClause;
      end if ;
       
    elsif (p_segment_type = 'INDEX PARTITION')
    then

      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild partition '|| p_partition_name ||' ' || getOnlineClause;

    elsif (p_segment_type = 'INDEX SUBPARTITION')
    then

      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild subpartition '|| p_partition_name ||' ' || getOnlineClause;

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
    select /*+ ALL_ROWS */ max(block_id) into b 
    from dba_extents where tablespace_name = p_tablespace_name ;
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

  procedure get_free_before_after(m in varchar2,i in varchar2,ts in varchar2,blk in number ,b in out number, a in out number) as
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
          where tablespace_name='TBS_BNA0PRD_BNA_ACTIVE' 
          and block_id < blk
         ) b
        ,(select  /*+ PARALLEL(8) */ sum(bytes)/1024/1024/1024 gb_after
          from dba_free_space 
          where tablespace_name='TBS_BNA0PRD_BNA_ACTIVE' 
          and block_id > blk
         ) a ;
     message(m || fmt1(b) || ' GB --> HWM <-- ' || fmt2(a) || ' GB'
            ,i,ts=>false) ;
  end ;

  procedure run_sql(command in varchar2) is
  --
  --  Run a SQL statement
  --
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
        message ('Success'
                ,'      +--> ',ts=>false);
      else
        message ('Test mode (command not ran)'
                ,'      +--> ',ts=>false);
      end if ;
    exception
    when others then
      message ('*** ERROR **  : ' || sqlerrm
              ,'      +--> ',ts=>false);
    end ;
    end_stmt := systimestamp ;
    message (''
            ,'         > ',ts=>false);
    message ('Time          : ' || (end_stmt -start_stmt)
            ,'         > ',ts=>false);
  end ;

  procedure abort_message(m in varchar2) is
  --
  --  Print a visible abord message
  --
  begin
    message ('',ts=>false);
    message (rpad('*',90,'*'),ts=>false);
    message('*** ' || rpad(m,80) || ' ****' ) ;
    message (rpad('*',90,'*'),ts=>false);
    message ('',ts=>false);
  end ;

-- -----------------------------------------------------------------------
--                       M A I N   P R O G R A M 
-- -----------------------------------------------------------------------

begin

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
  message (rpad('|  Max run duration      : ' || param_max_run_time || ' Hours'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Online/Offline        : ' || param_online
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Parallel              : ' || param_parallel
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Maximun Extents       : ' || param_max_rows
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Start Date            : ' || to_char(start_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);

  --
  --     Loop on the tablespaces depending on the criteria given (regexp format)
  --
  for tablespaces in ( select tablespace_name 
                             ,block_size
                       from   dba_tablespaces
                       where  regexp_like (tablespace_name,param_tablespace_name )
                       and    tablespace_name not in ('SYSTEM','SYSAUX') 
                       and    contents = 'PERMANENT' 
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
         )
    loop
      if (systimestamp > (start_run + (param_max_run_time/24)))
      then
        --
        --     If the script run for more than the allotted time, then
        -- we abort. 
        --
        --     Attention, if we run OFFLINE, there will still remain some
        -- time spent in the UNUSABLE indexes rebuild. When running ONLINE
        -- the indexes should have been maintained during the move operations.
        --
        abort_message('Aborting the script execution after ' || param_max_run_time || ' hours') ;
        abort_loop := true ;
        exit ;
      end if ;

      
      message('Segment : ' || rpad(nvl(extentsToMove.owner ||'.'|| extentsToMove.segment_name ,' '),50) || rpad('('||nvl(extentsToMOve.segment_type,' ')||')',30) || '(block : ' ||extentsToMove.block_id || ')' 
              ,' --+--> ');
            
      command := get_command(extentsToMove.owner,extentsToMove.segment_name,extentsToMove.partition_name
                            ,extentsToMove.segment_type,extentsToMove.block_id
                            ,tablespaces.tablespace_name);

      if (command is not null)
      then
        message (''
                ,'   | ',ts=>false);
        message ('Block : ' || fmt1(extentsToMove.block_id) --|| ' Last Extent Size: ' || fmt2(extent_size/1024/1024/1024) || 'GB'
                ,'   +--+--> ',ts=>false);
        message(''
                ,'      |  > ',ts=>false);
        get_free_before_after('Before move   : ','      |  > ',tablespaces.tablespace_name,extentsToMove.block_id,free_before_hwm1,free_after_hwm1) ;


        --
        --   Run the statement, errors in the move operation are simply printed, they
        -- do not stop the execution
        --
        run_sql (command) ;

        --
        --  Print new information about blocks and HWM
        --
        highest_block := getHighestBlock(tablespaces.tablespace_name) ;
        message ('Highest Block : ' || fmt1(highest_block) || ' variation (' || (highest_block-previous_highest_block) || ')'
                ,'         > ',ts=>false);
        get_free_before_after('After move    : ','         > ',tablespaces.tablespace_name,highest_block,free_before_hwm2,free_after_hwm2) ;

        --
        -- This is an indication of the gain.
        --
        message ('Variation     : ' || fmt2(free_after_hwm2 - free_after_hwm1) || ' GB'
                ,'         > ',ts=>false);

        if ((highest_block-previous_highest_block) > 0)
        then
          --
          --    if the segment has been moved up, then we stop here for this tablespace
          --
          abort_message('Aborting the script execution because used space doesn''t decrease anymore' ) ;
          exit ;
        end if ;
        previous_highest_block := highest_block ;
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


    --
    --    Print information on the tablespace (the new HWN should be lower than the initial one)
    --
    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    message (rpad('|  Initial highest block : ' || fmt1(initial_highest_block)
                  ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('|  Current highest block : ' || fmt1(highest_block)
                 ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('|  Variation             : ' || fmt1(highest_block-initial_highest_block) || ' Blocks'
                 ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    message('',ts=>false) ;

    --
    --     If we run OFFLINE, the indexes needs to be rebuilt, we do this on a 
    --  Tablespace basis. We could have put this after each move, but with the
    --  risk of rebuilding the same index several times.
    -- 
    message ('','',false) ;
    message ('- Rebuilding UNUSABLE Indexes ...','',false) ;
    message ('  -------------------------------','',false) ;
    message ('','',false) ;

    for rebuildCmds in ( select owner
                               ,index_name
                               ,'' partition_name 
                               ,'alter index ' || owner || '.' || index_name || ' rebuild' command 
                         from dba_indexes 
                         where status='UNUSABLE' 
                         and   owner in (select username from dba_users where oracle_maintained = 'N')
                         UNION
                         select index_owner
                               ,index_name,partition_name
                               ,'alter index ' || index_owner || '.' || index_name || ' rebuild partition ' || partition_name 
                         from dba_ind_partitions 
                         where status='UNUSABLE' 
                         and   index_owner in (select username from dba_users where oracle_maintained = 'N')
                       )
    loop
      command := rebuildCmds.command || ' ' || getOnlineClause ;
      message('Rebuild : ' || rebuildCmds.owner || '.' || rebuildCmds.index_name || 
              case when rebuildCmds.partition_name is null then '' else '(Part : ' || rebuildCmds.partition_name || ')' end 
              ,' --+--> ');
      run_sql(command) ;
    end loop ;

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

--
--   Remove the temporary file since the same output was printed on stdout
--
host rm -f &RT_DIR/&RT_LOG

prompt
prompt =========================================================================
Prompt .         High Water Mark analysis (After run) 
Prompt .         Commands to run to resize the datafiles if needed
prompt =========================================================================
Prompt

column current_size_gb          format 999G990D00     heading "Current Size (GB)"
column hwm_gb                   format 999G990D00     heading "Hwm (GB)"
column savings_gb               format 999G990D00     heading "Possible Savings (GB)"
column command                  format a200           heading "Resize Command"

break on report
compute sum of current_size_gb on report
comput sum of savings_gb on report

SELECT 
    ceil(blocks *(a.blocksize) / 1024 / 1024 / 1024)                                                                               current_size_gb,
    ceil((nvl(hwm, 1) *(a.blocksize)) / 1024 / 1024 / 1024)                                                                        hwm_gb,
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
    savings_gb DESC
/

