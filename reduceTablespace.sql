set serveroutput on 
declare
  param_tablespace_name varchar2(100) := 'TBS_BNA0PRD_LIQF2_ACTIVE' ;
  param_max_rows number := 3 ;

  command varchar2(1000) ;
  
  start_run  timestamp ;
  end_run    timestamp ;
  start_stmt timestamp ;
  end_stmt   timestamp ;
  
  highest_block number ;
  previous_highest_block number ;
  initial_highest_block number ;

  procedure message(m in varchar2,indent in varchar2 default ' ',ts in boolean default true ) is
    dat varchar2(30) := to_char(current_timestamp,'dd/mm/yyyy hh24:mi:ss.FF3') ;
  begin
    if not ts then dat := '.                      ' ; end if ;
    dbms_output.put_line( dat || indent || m );
  end ;
                function get_command(p_owner in varchar2,p_segment_name in varchar2 ,p_partition_name in varchar2,p_segment_type in varchar2,p_block_id in number ) return varchar2 is
                  tmp varchar2(2000) := '' ; 
                  lp_table_owner        dba_lob_partitions.table_owner%type ;
                  lp_table_name         dba_lob_partitions.table_name%type ;
                  lp_partition_name     dba_lob_partitions.partition_name%type ;
                  lp_lob_partition_name dba_lob_partitions.lob_partition_name%type ;
                  lp_lob_name           dba_lob_partitions.lob_name%type ;
                  lp_tablespace_name    dba_lob_partitions.tablespace_name%type ;
            
                  in_partitioned        dba_indexes.partitioned%type ;
            
                  ta_partitioned        dba_tables.partitioned%type ;
                  
                  lo_table_name         dba_lobs.table_name%type ;
                  lo_column_name        dba_lobs.column_name%type ;
                  
                begin
                  if ( p_segment_type = 'LOB PARTITION')
                  then
                    select table_owner    , table_name    , partition_name    , lob_partition_name    , lob_name    , tablespace_name
                    into   lp_table_owner , lp_table_name , lp_partition_name , lp_lob_partition_name , lp_lob_name , lp_tablespace_name
                    from dba_lob_partitions 
                    where lob_partition_name = p_partition_name and table_owner=p_owner ;  
            
                    select table_name    , COLUMN_NAME
                    into   lo_table_name , lo_column_name
                    from dba_lobs where segment_name=lp_lob_name ;
                    tmp :=  'alter table ' || lp_table_owner || '.' || lo_table_name || ' move partition ' ||lp_partition_name ||
                            ' lob(' || lo_column_name || ') store as (tablespace '|| lp_tablespace_name || ') online parallel 16 ' ;
                  elsif ( p_segment_type = 'LOBSEGMENT')
                  then
                    select table_name    , COLUMN_NAME
                    into   lo_table_name , lo_column_name
                    from dba_lobs where segment_name=p_segment_name ;
                    tmp :=  'alter table ' || p_owner || '.' || lo_table_name || ' move ' ||
                            ' lob(' || lo_column_name || ') store as (tablespace '|| param_tablespace_name || ') online parallel 16 ' ;
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
                      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move online parallel 16 ' ;
                    end if ;
                  elsif (p_segment_type = 'TABLE PARTITION')
                  then
                      tmp := 'alter table ' || p_owner || '.' || p_segment_name || ' move partition '|| p_partition_name ||' online parallel 16 ' ;
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
                      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild online parallel 16 ' ;
                    end if ;
                  elsif (p_segment_type = 'INDEX PARTITION')
                  then
                      tmp := 'alter index ' || p_owner || '.' || p_segment_name || ' rebuild partition '|| p_partition_name ||' online parallel 16 ' ;
                  end if ;
                  return tmp ;
                exception when others then
                  return tmp || ' ' || sqlerrm ;
                end ;


  function getHighestBlock (p_tablespace_name in varchar2) return number is
    b number ;
  begin
    select max(block_id) into b 
    from dba_extents where tablespace_name = p_tablespace_name ;
    return (b) ;
  end ;
begin

  start_run := systimestamp ;
  highest_block := getHighestBlock(param_tablespace_name) ;
  previous_highest_block := highest_block ;
  initial_highest_block := highest_block ;
  
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|          Move upper extents down in the tablespace'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  Tablespace            : ' || param_tablespace_name
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Initial highest block : ' || to_char(initial_highest_block,'999G999G999G999G999')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Maximun Extents       : ' || param_max_rows
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Start Date            : ' || to_char(start_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);
  for extentsToMove in (
            with 
            high_segs as (
                              select
                                 owner
                                ,segment_name
                                ,partition_name
                                ,segment_type
                                ,max(block_id) block_id
                              from
                                 dba_extents
                              where
                                tablespace_name = param_tablespace_name
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
    message('Segment : ' || rpad(nvl(extentsToMove.owner ||'.'|| extentsToMove.segment_name ,' '),50) || rpad('('||nvl(extentsToMOve.segment_type,' ')||')',30) ||
            '(block : ' ||extentsToMove.block_id || ')' 
            ,' --+--> ');
            
    command := get_command(extentsToMove.owner,extentsToMove.segment_name,extentsToMove.partition_name,extentsToMove.segment_type,extentsToMove.block_id);
    if (command is not null)
    then
      message (''
              ,'   | ',ts=>false);
      message ('Running :' || command
              ,'   +--+--> ',ts=>false);
      message (''
              ,'      | ',ts=>false);
      start_stmt := systimestamp ;
      begin
        execute immediate command ;
          message ('Success'
                  ,'      +--> ',ts=>false);
      exception
        when others then
          message ('*** ERROR **  : ' || sqlerrm
                  ,'      +--> ',ts=>false);
      end ;
      end_stmt := systimestamp ;
      message ('Time          : ' || (end_stmt -start_stmt)
              ,'         > ',ts=>false);
      highest_block := getHighestBlock(param_tablespace_name) ;
      message ('Highest Block : ' || to_char(highest_block,'999G999G999G999G999') || ' variation (' || (highest_block-previous_highest_block) || ')'
              ,'         > ',ts=>false);
      previous_highest_block := highest_block ;
    else
      message (' *** Unable to process ' || extentsToMOve.segment_type
              ,'   +--+--> ',ts=>false);
    end if ;
      message (''
              ,'',ts=>false);
  end loop ;
  
  end_run := systimestamp ;
  
  message ('',ts=>false);
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message (rpad('|  Initial highest block : ' || to_char(initial_highest_block,'999G999G999G999G999')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Current highest block : ' || to_char(highest_block,'999G999G999G999G999')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Variation             : ' || to_char(highest_block-initial_highest_block,'999G999G999G999G999') || ' Blocks'
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  End of run            : ' || to_char(end_run,'dd/mm/yyyy hh24:mi:ss')
               ,80,' ') || '|',ts=>false) ;
  message (rpad('|  Run duration          : ' || (end_run - start_run)
               ,80,' ') || '|',ts=>false) ;
  message (rpad('+',80,'-') || '+',ts=>false) ;
  message ('',ts=>false);

end ;
/
