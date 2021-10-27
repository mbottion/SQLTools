Rem
Rem If queries against dictionary becomes slow:
rem exec dbms_stats.gather_dictionary_stats ;
rem exec dbms_stats.gather_fixed_objects_stats ;
rem 

set serveroutput on 
declare
  param_tablespace_name varchar2(100)   := '.*_TGP_.*|.*_TExC_.*' ;
  param_max_rows number                 := 1 ;
  param_max_run_time number             := 6 ;
  param_real_time_log_dir varchar2(100) := '/tmp' ;
  param_run_it boolean                  := true ;

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


                function get_command(p_owner in varchar2,p_segment_name in varchar2 ,p_partition_name in varchar2,p_segment_type in varchar2,p_block_id in number ,dest_tablespace in varchar2) return varchar2 is
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
                    select table_owner    , table_name    , partition_name    , lob_partition_name    , lob_name    
                    into   lp_table_owner , lp_table_name , lp_partition_name , lp_lob_partition_name , lp_lob_name 
                    from dba_lob_partitions 
                    where lob_partition_name = p_partition_name and table_owner=p_owner ;  
            
                    select table_name    , COLUMN_NAME
                    into   lo_table_name , lo_column_name
                    from dba_lobs where segment_name=lp_lob_name ;
                    tmp :=  'alter table ' || lp_table_owner || '.' || lo_table_name || ' move partition ' ||lp_partition_name ||
                            ' lob(' || lo_column_name || ') store as (tablespace '|| dest_tablespace || ') parallel 32 ' ;
                  elsif ( p_segment_type = 'LOBSEGMENT')
                  then
                    select table_name    , COLUMN_NAME
                    into   lo_table_name , lo_column_name
                    from dba_lobs where segment_name=p_segment_name ;
                    tmp :=  'alter table ' || p_owner || '.' || lo_table_name || ' move ' ||
                            ' lob(' || lo_column_name || ') store as (tablespace '|| dest_tablespace || ') parallel 32 ' ;
                  elsif (p_segment_type = 'TABLE')
                  then
                    select partitioned
                    into   ta_partitioned
                    from dba_tables 
                    where owner = p_owner and table_name = p_segment_name ;
                    if ( ta_partitioned = 'YES' )
                    then
                      tmp := tmp || '/* partitioned table */' ;
                    else
                      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move parallel 32 ' ;
                    end if ;
                  elsif (p_segment_type = 'TABLE PARTITION')
                  then
                      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move partition '|| p_partition_name ||' parallel 32 ' ;
                  elsif (p_segment_type = 'TABLE SUBPARTITION')
                  then
                      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move subpartition '|| p_partition_name ||' parallel 32 ' ;
                  elsif (p_segment_type = 'INDEX')
                  then
                    select partitioned
                    into   in_partitioned
                    from dba_indexes 
                    where owner = p_owner and index_name = p_segment_name ;
                    if ( in_partitioned = 'YES' )
                    then
                      tmp := tmp || '/* partitioned index */' ;
                    else
                      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild parallel 32 ' ;
                    end if ;
                  elsif (p_segment_type = 'INDEX PARTITION')
                  then
                      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild partition '|| p_partition_name ||' parallel 32 ' ;
                  elsif (p_segment_type = 'INDEX SUBPARTITION')
                  then
                      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild subpartition '|| p_partition_name ||' parallel 32 ' ;
                  end if ;
                  return tmp ;
                exception when others then
                  return tmp || ' ' || sqlerrm ;
                end ;


  procedure message(m in varchar2,indent in varchar2 default ' ',ts in boolean default true ) is
    dat varchar2(30) := to_char(current_timestamp,'dd/mm/yyyy hh24:mi:ss.FF3') ;
    mess varchar2(2000) ;
    fh utl_file.file_type ;
  begin
    if not ts then dat := '.                      ' ; end if ;
    mess := dat || indent || m ;
    dbms_output.put_line( mess );
    if ( ora_log_dir is not null )
    then
      if first_call
      then
        fh := utl_file.fopen ( ora_log_dir,'reduceTablespace.log','w') ;
        first_call := false ;
      else
        fh := utl_file.fopen ( ora_log_dir,'reduceTablespace.log','a') ;
      end if ;
      utl_file.put_line ( fh, mess) ;
      utl_file.fclose (fh) ;
    end if ;
  end ;

  function getHighestBlock (p_tablespace_name in varchar2) return number is
    b number ;
  begin
    select /*+ ALL_ROWS */ max(block_id) into b 
    from dba_extents where tablespace_name = p_tablespace_name ;
    return (nvl(b,0)) ;
  end ;

  function createDir(physical_dir in varchar2) return varchar2 as
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
  function fmt1(n number) return varchar2 is begin return(to_char(n,'999G999G999G999G990')) ; end ;
  function fmt2(n number) return varchar2 is begin return(to_char(n,'999G990D00')) ; end ;
  procedure get_free_before_after(m in varchar2,i in varchar2,ts in varchar2,blk in number ,b in out number, a in out number) as
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

begin

  ora_log_dir := createDir(param_real_time_log_dir) ;
  message('','',ts=>false);

  start_run := systimestamp ;
  
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|          Move upper extents down in the tablespace'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  Maximun Extents       : ' || param_max_rows
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Start Date            : ' || to_char(start_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);
  for tablespaces in ( select tablespace_name 
                             ,block_size
                       from   dba_tablespaces
                       where  regexp_like (tablespace_name,param_tablespace_name )
                       and    tablespace_name not in ('SYSTEM','SYSAUX') 
                       and    contents = 'PERMANENT' )
  loop


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
        message ('',ts=>false);
        message('      *** Aborting the script execution after ' || param_max_run_time || ' hours' ) ;
        message ('',ts=>false);
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
    --    select /* + ALL_ROWS */ bytes
    --    into   extent_size
    --    from   dba_extents
    --    where  tablespace_name = tablespaces.tablespace_name
    --    and    block_id = extentsToMove.block_id ;
        message (''
                ,'   | ',ts=>false);
        message ('Block : ' || fmt1(extentsToMove.block_id) --|| ' Last Extent Size: ' || fmt2(extent_size/1024/1024/1024) || 'GB'
                ,'   +--+--> ',ts=>false);
        message(''
                ,'      |  > ',ts=>false);
        get_free_before_after('Before move   : ','      |  > ',tablespaces.tablespace_name,extentsToMove.block_id,free_before_hwm1,free_after_hwm1) ;


        run_sql (command) ;

        highest_block := getHighestBlock(tablespaces.tablespace_name) ;
        message ('Highest Block : ' || fmt1(highest_block) || ' variation (' || (highest_block-previous_highest_block) || ')'
                ,'         > ',ts=>false);
        get_free_before_after('After move    : ','         > ',tablespaces.tablespace_name,extentsToMove.block_id,free_before_hwm2,free_after_hwm2) ;
        message ('Variation     : ' || fmt2(free_after_hwm2 - free_after_hwm1) || ' GB'
                ,'         > ',ts=>false);
        if ((highest_block-previous_highest_block) > 0)
        then
          message('      *** Aborting the script execution because used space doesn''t decrease anymore' ) ;
          message ('',ts=>false);
          abort_loop := true ;
          exit ;
        end if ;
        previous_highest_block := highest_block ;
      else
        message (' *** Unable to process ' || extentsToMOve.segment_type
                ,'   +--+--> ',ts=>false);
      end if ;
        message (''
                ,'',ts=>false);
    end loop ;

    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    message (rpad('|  Initial highest block : ' || fmt1(initial_highest_block)
                  ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('|  Current highest block : ' || fmt1(highest_block)
                 ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('|  Variation             : ' || fmt1(highest_block-initial_highest_block) || ' Blocks'
                 ,80,' ') || '|','    ',ts=>false) ;
    message (rpad('+',80,'-') || '+','    ',ts=>false) ;
    message('',ts=>false) ;

    message ('','',false) ;
    message ('- Rebuilding UNUSABLE Indexes ...','',false) ;
    message ('  -------------------------------','',false) ;
    message ('','',false) ;

    for rebuildCmds in ( select owner
                               ,index_name
                               ,'' partition_name 
                               ,'alter index ' || owner || '.' || index_name || ' rebuild parallel 32 ' command 
                         from dba_indexes 
                         where status='UNUSABLE' 
                         and   owner in (select username from dba_users where oracle_maintained = 'N')
                         UNION
                         select index_owner
                               ,index_name,partition_name
                               ,'alter index ' || index_owner || '.' || index_name || ' rebuild partition ' || partition_name || ' parallel 32 ' 
                         from dba_ind_partitions 
                         where status='UNUSABLE' 
                         and   index_owner in (select username from dba_users where oracle_maintained = 'N')
                       )
    loop
      message('Rebuild : ' || rebuildCmds.owner || '.' || rebuildCmds.index_name || 
              case when rebuildCmds.partition_name is null then '' else '(Part : ' || rebuildCmds.partition_name || ')' end 
              ,' --+--> ');
      run_sql(rebuildCmds.command) ;
    end loop ;

    if ( abort_loop )
    then
      exit ;
    end if;
  end loop ;

  end_run := systimestamp ;
  
  message ('',ts=>false);
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  End of run            : ' || to_char(end_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Run duration          : ' || (end_run - start_run)
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);

end ;
/
Rem
Rem If queries against dictionary becomes slow:
rem exec dbms_stats.gather_dictionary_stats ;
