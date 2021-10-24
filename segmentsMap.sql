-- --------------------------------------------------------------------------------------
--
--     segment map (TXT or CSV to be processed via a MACRO in EXCEL)
--
-- --------------------------------------------------------------------------------------

-- -----------------------------------------------------------------
-- parameters
-- -----------------------------------------------------------------

--
-- name of the tablespace to analyze, and optinally the segment(s) (like format)
--


define tablespace=DATA
define segment_name=%


--    TXT are sufficient for a global analysis, set the number of sample low to view them easily on a screen
--
--    For cleaner results, choose CSV an reprocess the output file in EXCEL via the macro in formatTBSAnalysis.xlsm
-- The maximum possible number of samples is 16300 (column limit un EXCEL)
-- Between 1000 and 5000 should be fine for most usages
-- If output is TXT, simple table output, if CSV, all fields are semi-column separated.
--
-- Values : TXT / CSV
--

define output_format=TXT

--
--    Output format
--
--     Number of of columns of the OUTPUT (Excel : Max=16 384 columns and 
-- block RANGE to analyze if we deal with large datafiles, to analyze the whole
-- files set lower_block=0 and upper_block=LAST_BLOCK
--
--     sample_count_txt     : Number of output columns for the map when generating TXT output
--     sample_count_csv     : Number of output columns for the map when generating CSV output
--     lower_block          : Begin analysis here
--     upper_block          : Begin analysis here
--     
--     convert_lob_names    : If Y, Replaces LOB Names by table.column (truncated when TXT)
--     group_partitions     : If Y Treat all partitions as a single object (to limit the number of lines
--     show_nums_in_txt     : If N, do not output the start, end blocks and the number of blocks
--
define sample_count_txt=100
define sample_count_csv=16300
define lower_block=FIRST_BLOCK
define upper_block=LAST_BLOCK

define convert_lob_names=Y
define group_partitions=N

define show_nums_in_txt=N

--
-- Determine the number of blocks of the largest datafile of the tablespace
--
col largest_file    noprint new_value largest_file 
col largest_file_mb noprint new_value largest_file_mb 
select 
   max(blocks)                        largest_file
  ,max(blocks * block_size)/1024/1204 largest_file_mb
from 
   v$datafile
where 
   file# in (select file_id from dba_data_files where tablespace_name = '&tablespace');

--
--  Calculated, based on output_format and previous parameters
--

-- -----------------------------------------------------------------
-- Determine the output filename
-- -----------------------------------------------------------------

col file_name format a100 noprint new_value file_name
select '/tmp/tbsMap_' || to_char(sysdate,'yyyymmdd_hh24miss') || '_' || lower(d.name) || '_' 
                      || replace(sys_context('USERENV','CON_NAME'),'$','_') 
                      || '_&tablespace'
                      || lower('.&output_format') file_name 
from dual,v$database d ;
alter session set nls_numeric_characters=', ' ;

set echo off termout on feedback on pages 0 heading off verify off lines 32767 sqlprompt "SQL> " trimout on trimspool on
set timing on 

-- -----------------------------------------------------------------

set serveroutput on 

spool &file_name

declare
  
  tmp                  varchar2(32767) := ' ' ;
  prev_owner           varchar2(100) ;
  prev_segment_name    varchar2(100) ;
  prev_segment_type    varchar2(100) ;
  prev_len_file        number ;
  prev_start_block     number ;
  prev_end_block       number ;
  prev_blocks          number ;
  prev_size_mb         number ;
  nb_rows              number := 0;
  sample_count         number := case when upper('&output_format') = 'TXT' then &sample_count_txt else &sample_count_csv end ;
  lower_block_number   number := case when upper('&lower_block')='FIRST_BLOCK' then '0'             else '&lower_block' end ;
  upper_block_number   number := case when upper('&upper_block')='LAST_BLOCK'  then '&largest_file' else '&upper_block' end;
  analyzed_blocks      number := upper_block_number - lower_block_number ;
  sample_size          number := ceil(analyzed_blocks / sample_count);

  function paddedString(str in varchar2,len in number, car in varchar2) return varchar2 is
  begin
    if ( upper('&output_format') = 'TXT' )
    then
      return(rpad(str,len,car) ||  ' ') ;
    else
      return(str||';') ;
    end if ;
  end ;
begin
  dbms_output.enable(buffer_size => null) ;

      dbms_output.put_line(rpad('-',50,'-')) ;
      dbms_output.put_line(paddedString('Tablespace MAP analysis',50,' ') );
      dbms_output.put_line(rpad('-',50,'-')) ;
      dbms_output.put_line('.') ;
      dbms_output.put_line(paddedString('Analysis Date',50,' ')                 || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss')   );
      dbms_output.put_line(paddedString('Tablespace',50,' ')                    || '&tablespace'   );
      dbms_output.put_line(paddedString('Segment Selection',50,' ')             || '&segment_name' );
      dbms_output.put_line(paddedString('Group partitions together',50,' ')     || '&group_partitions' );
      dbms_output.put_line(paddedString('Largest File (Blocs)',50,' ')          || '&largest_file' );
      dbms_output.put_line(paddedString('Largest File (MB) = Map width',50,' ') || '&largest_file_mb' );
      dbms_output.put_line('.') ;
      dbms_output.put_line(paddedString('Start analysis at block',50,' ')       || lower_block_number  );
      dbms_output.put_line(paddedString('End analysis at block',50,' ')         || upper_block_number  );
      dbms_output.put_line(paddedString('Number of samples',50,' ')             || sample_count  );
      dbms_output.put_line(paddedString('Analyzed Blocks',50,' ')               || analyzed_blocks  );
      dbms_output.put_line(paddedString('Sample Size (in blocks)',50,' ')       || sample_size  );
      dbms_output.put_line('.') ;
      
      dbms_output.put_line(paddedString('Owner'       ,20,' ')||
                           paddedString('Segment Name',40,' ')||
                           paddedString('Segment Type',20,' ')||
                           case
                             when upper('&output_format') != 'TXT' then
                               paddedString('Start Block' ,15,' ') ||
                               paddedString('End Block'   ,15,' ') ||
                               paddedString('Blocks'      ,15,' ') ||
                               paddedString('Size MB'     ,15,' ') 
                             else 
                               ''
                           end ||
                           paddedString('Map : 1 car = ' || sample_size || ' Blocks' , sample_count +2 , ' ') ||
                           case
                             when upper('&output_format') = 'TXT' and upper('&show_nums_in_txt') = 'Y' then 
                               paddedString('Start Block' ,15,' ') ||
                               paddedString('End Block'   ,15,' ') ||
                               paddedString('Blocks'      ,15,' ') ||
                               paddedString('Size MB'     ,15,' ') 
                             else 
                               ''
                           end
                           ) ;
      if ( upper('&output_format') = 'TXT' )
      then
      dbms_output.put_line(paddedString('-'           ,20,'-')||
                           paddedString('-'           ,40,'-')||
                           paddedString('-'           ,20,'-')||
                           paddedString('-'           ,sample_count+2,'-') ||
                           case
                           when upper('&output_format') = 'TXT' and upper('&show_nums_in_txt') = 'Y' then 
                             paddedString('-'           ,15,'-') ||
                             paddedString('-'           ,15,'-') ||
                             paddedString('-'           ,15,'-') ||
                             paddedString('-'           ,15,'-') 
                           else
                             ''
                           end
                           ) ;
      end if ;

for rec$map in (
                            with orderedExtents as (
                                        SELECT /*+ PARALLEL(8) */
                                            --
                                            --      get the extents ans order them by file_is and block_id
                                            --  do not appy the block restriction here since the statement becomes very long
                                            --
                                            de.owner
                                           ,case 
                                              when de.segment_type in ('LOBSEGMENT','LOB PARTITION') /* Convert LOB Names to table.column */
                                              and  de.segment_name like 'SYS%$$'
                                              and  upper('&convert_lob_names') = 'Y'
                                              then (select lower(substr(table_name,1,case when upper('&output_format') = 'TXT' then 10 else 30 end ) || '.' || 
                                                                 substr(column_name,1,case when upper('&output_format') = 'TXT' then 10 else 30 end)
                                                                ) 
                                                    from   dba_lobs 
                                                    where  segment_name = de.segment_name 
                                                    and    owner = de.owner)
                                              when de.segment_type in ('LOBINDEX','INDEX PARTITION') /* Convert LOB INDEX Names to table.column */
                                              and upper('&convert_lob_names') = 'Y'
                                              and  de.segment_name like 'SYS_IL%$$'
                                              and  upper('&convert_lob_names') = 'Y'
                                              then (select lower(substr(table_name,1,case when upper('&output_format') = 'TXT' then 10 else 30 end ) || '.' || 
                                                                 substr(column_name,1,case when upper('&output_format') = 'TXT' then 10 else 30 end)
                                                                ) 
                                                    from   dba_lobs 
                                                    where  index_name = de.segment_name 
                                                    and    owner = de.owner)
                                              else de.segment_name
                                            end segment_name
                                           ,case 
                                             when      de.partition_name is not null 
                                                  and  upper('&group_partitions') = 'Y'
                                             then 'AllPartsGrouped'
                                             else de.partition_name
                                            end partition_name
                                           ,de.segment_type
                                           ,de.file_id
                                           ,de.block_id                                                        start_block
                                           ,de.block_id + de.blocks                                            end_block
                                           ,de.blocks
                                           ,de.extent_id
                                           ,df.block_size
                                        FROM
                                            dba_extents de
                                        JOIN v$datafile df on (de.file_id = df.file#)
                                        WHERE 1=1
                                        AND   de.segment_name like '&segment_name'
                                        AND   de.file_id in (select file_id
                                                          from   dba_data_files 
                                                          where  tablespace_name = upper('&tablespace'))
                                        order by
                                          file_id, block_id
                            )
                            ,groupedExtents as (
                              --
                              --    Create a FAKE extent for several contiguous extents
                              -- could ave been above, but it's easier to understand like this !
                              --
                              select 
                                 owner
                                ,segment_name ||
                                 case
                                   when partition_name is null then ''
                                   else ':' || partition_name
                                 end                                           segment_name
                                ,segment_type
                                ,file_id
                                ,block_size
                                ,min(start_block)                              start_block
                                ,max(end_block)-1                              end_block
                                ,sum(blocks)                                   blocks
                              from 
                                orderedExtents
                              group by
                                 owner
                                ,segment_name
                                ,partition_name
                                ,segment_type
                                ,file_id
                                ,block_size
                            )
                            ,segmentMapParts1 as (
                              --
                              --   Prepares the data to print each extent on the map, those figures will be used in the PL/SQL loop
                              -- to build strings with ' ' anf 'X's
                              --
                              select 
                                 ge.owner
                                ,ge.segment_name
                                ,ge.segment_type
                                ,ceil(ge.start_block*(sample_count/analyzed_blocks))-1                          map_pad_left
                                ,ceil((ge.end_block - start_block)*(sample_count/analyzed_blocks))              map_blocks
                                ,ceil(  ge.start_block*(sample_count/analyzed_blocks))-1 
                                      + ceil((ge.end_block - ge.start_block)*(sample_count/analyzed_blocks))    map_len
                                ,lag(  ceil(ge.start_block*(sample_count/analyzed_blocks))-1 
                                     + ceil((ge.end_block - ge.start_block)*(sample_count/analyzed_blocks)),1,0
                                    )
                                    over (partition by  ge.file_id
                                                       ,ge.owner
                                                       ,ge.segment_name
                                                       ,ge.segment_type 
                                          order by ge.start_block)                                                map_prev_len
                                ,ge.start_block
                                ,ge.end_block
                                ,ge.blocks
                                ,(ge.blocks*ge.block_size)/1024/1024                                             seg_size_MB
                                ,fi.blocks file_blocks
                                ,ceil((least(fi.blocks,analyzed_blocks)/analyzed_blocks)*sample_count)         map_len_file
                              from
                                groupedExtents ge
                              join dba_data_files fi on (ge.file_id = fi.file_id)
                            order by 
                               owner
                              ,segment_name
                              ,segment_type
                              ,start_block
                            )
                            select * from segmentMapParts1
                            where start_block between lower_block_number and upper_block_number - blocks
                          ) 
  loop
    if ( rec$map.map_prev_len = 0 )
    then
      --
      --    For each new group, prin the MAP of the previous group
      --
      if (  tmp != ' ' )
      then
        dbms_output.put_line(paddedString(prev_owner       ,20,' ') || 
                             paddedString(prev_segment_name,40,' ') || 
                             paddedString(prev_segment_type,20,' ') || 
                             case
                               when upper('&output_format') != 'TXT' then
                                 paddedString(prev_start_block ,15,' ') ||
                                 paddedString(prev_end_block   ,15,' ') ||
                                 paddedString(prev_blocks      ,15,' ') ||
                                 paddedString(prev_size_mb     ,15,' ') 
                               else 
                                 ''
                             end ||
                             replace('['|| case when upper('&output_format') = 'TXT' then '' else ';' end||rpad(tmp,prev_len_file
                                                  ,case when upper('&output_format') = 'TXT' then ' ' else ';' end
                                              ) || ']' 
                                    ,'X'
                                    ,case when upper('&output_format') = 'TXT' then 'X' else 'X;' end 
                                    ) ||
                             case
                             when upper('&output_format') = 'TXT' and upper('&show_nums_in_txt') = 'Y' then 
                                 ' ' ||
                                 paddedString(prev_start_block ,15,' ') ||
                                 paddedString(prev_end_block   ,15,' ') ||
                                 paddedString(prev_blocks      ,15,' ') ||
                                 paddedString(prev_size_mb     ,15,' ') 
                               else 
                                 ''
                             end 
                            );
        nb_rows := nb_rows + 1 ;
      end if ;
      tmp := case when upper('&output_format') = 'TXT' then ' ' else ';' end ;
      prev_owner := rec$map.owner ;
      prev_segment_name := rec$map.segment_name ;
      prev_segment_type := rec$map.segment_type ;
      prev_len_file     := rec$map.map_len_file ;
      prev_start_block  := rec$map.start_block  ;
      prev_end_block    := rec$map.end_block    ;
      prev_blocks       := rec$map.blocks       ;
      prev_size_mb      := rec$map.seg_size_mb  ;
    end if ; 
    --
    --    Add current extent at the right position in the string
    --
    if ( upper('&output_format' ) = 'TXT' )
    then
      tmp := rpad(rpad(tmp,rec$map.map_pad_left,' '),rec$map.map_len,'X') ;
    else
      tmp := rpad(rpad(tmp,rec$map.map_pad_left,';'),rec$map.map_len,'X') ;
    end if ;
  end loop ;
  --
  --      Print last line of the map
  --
        dbms_output.put_line(paddedString(prev_owner       ,20,' ') || 
                             paddedString(prev_segment_name,40,' ') || 
                             paddedString(prev_segment_type,20,' ') || 
                             case
                               when upper('&output_format') != 'TXT' then
                                 paddedString(prev_start_block ,15,' ') ||
                                 paddedString(prev_end_block   ,15,' ') ||
                                 paddedString(prev_blocks      ,15,' ') ||
                                 paddedString(prev_size_mb     ,15,' ') 
                               else 
                                 ''
                             end ||
                             replace('['|| case when upper('&output_format') = 'TXT' then '' else ';' end||rpad(tmp,prev_len_file
                                                  ,case when upper('&output_format') = 'TXT' then ' ' else ';' end
                                              ) || ']' 
                                    ,'X'
                                    ,case when upper('&output_format') = 'TXT' then 'X' else 'X;' end 
                                    ) ||
                             case
                             when upper('&output_format') = 'TXT' and upper('&show_nums_in_txt') = 'Y' then 
                                 ' ' ||
                                 paddedString(prev_start_block ,15,' ') ||
                                 paddedString(prev_end_block   ,15,' ') ||
                                 paddedString(prev_blocks      ,15,' ') ||
                                 paddedString(prev_size_mb     ,15,' ') 
                               else 
                                 ''
                             end 
                            );
     dbms_output.put_line ('.') ;
     dbms_output.put_line (nb_rows || ' Objects Selected') ;
     dbms_output.put_line ('.') ;
     dbms_output.put_line (paddedString('Analysis End',50,' ') || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss')   );
end ;
/

