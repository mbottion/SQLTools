Rem
Rem  List of segments by highest blockid descending
Rem
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
                        tablespace_name = 'TBS_BNA0PRD_BNA_ACTIVE'
                    and segment_name like upper('%')
                    and segment_type != 'TEMPORARY'
                  group by
                     owner
                    ,segment_name
                    ,partition_name
                    ,segment_type
                  order by block_id desc
                   )
select
     owner
    ,segment_name
    ,partition_name
    ,segment_type
    ,block_id
    ,to_char((block_id*8192)/1024/1024/1024,'999G990D99') "Position_GB"
from
  high_segs
order by block_id desc
/
