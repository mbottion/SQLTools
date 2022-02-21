
define past_days=30
define schemas_list="'BNA','TEC','LIQ1','LIQ2','LIQF1','LIQF2','SYN1','ACE1','TGP'"
define objects_name=%
define subobjects_name=%
--define objects_name=RES_RESSOURCE

define output_header=Y
define output_absolute=Y
define output_pct=Y
define output_missing=Y

set lines 5000
set pages 10000
set tab off
set trimspool on trimout on recsep off 
set heading off

column ord                         noprint
column var_type        format a20
column owner           format a10
column object_name     format a30
column subobject_name  format a30
column tablespace_name format a30
column today           format a15
column d1              format a10
column d2              format a10
column d3              format a10
column d4              format a10
column d5              format a10
column d6              format a10
column d7              format a10
column d8              format a10
column d9              format a10
column d10             format a10
column d11             format a10
column d12             format a10
column d13             format a10
column d14             format a10
column d15             format a10
column d16             format a10
column d17             format a10
column d18             format a10
column d19             format a10
column d20             format a10
column d21             format a10
column d22             format a10
column d23             format a10
column d24             format a10
column d25             format a10
column d26             format a10
column d27             format a10
column d28             format a10
column d29             format a10
column d30             format a10

with 
  segment_growth as (
    SELECT
--       seh.dbid
--      ,seh.obj#
--      ,seh.dataobj#
       obj.owner                                        
      ,obj.tablespace_name
      ,decode(obj.object_type
             ,'LOB','LOBSEGMENT'
             ,obj.object_type)                             object_type
      ,obj.object_name
      ,obj.subobject_name
      ,trunc(nvl(snp.end_interval_time,sysdate))           snap_day
      ,round(sum(seh.space_used_delta) / 1024 / 1024,4)    size_variation_mb
    FROM
       dba_hist_snapshot      snp
      ,dba_hist_seg_stat      seh
      ,dba_hist_seg_stat_obj  obj
      ,v$database             db
    WHERE 
            snp.begin_interval_time (+) > trunc(sysdate) - &past_days 
--        and snp.instance_number=1
        and snp.dbid (+) = db.dbid 
        -- ------------------------------------------------------
        AND snp.snap_id             (+) = seh.snap_id 
        and snp.dbid                (+) = seh.dbid 
        and snp.instance_number     (+) = seh.instance_number 
        -- ------------------------------------------------------
        AND obj.obj#                = seh.obj#
        AND obj.dataobj#            = seh.dataobj#
        and obj.dbid                = seh.dbid
        -- ------------------------------------------------------
        AND obj.object_name NOT LIKE 'BIN$%' -- recycle-bin
        and obj.owner                      in (&schemas_list)
        and obj.object_name                like '&objects_name'
        and nvl(obj.subobject_name,'MAIN') like '&subobjects_name'
    group by 
       obj.owner
      ,obj.tablespace_name
      ,obj.object_type
      ,obj.object_name
      ,obj.subobject_name
      ,trunc(nvl(snp.end_interval_time,sysdate))
  )
  ,segment_pre_pivot as
  (
    select 
       sgr.owner
      ,sgr.tablespace_name
      ,sgr.object_type
      ,sgr.object_name
      ,sgr.subobject_name
      ,case sgr.snap_day 
        when trunc(sysdate) then 'Today'
        else 'D-' || to_char(trunc(sysdate) - snap_day)
       end                                                       snap_day
      ,seg.bytes/1024/1024                                       current_size_mb
      ,sgr.size_variation_mb
      ,round((sgr.size_variation_mb / (seg.bytes/1024/1024 )),2) pct_of_current_size_variation
    from 
      segment_growth sgr
    join DBA_SEGMENTS seg on (    sgr.owner                          = seg.owner
                              and sgr.object_name                    = seg.segment_name
                              and nvl(sgr.subobject_name,'$$MAIN$$') = nvl(seg.partition_name,'$$MAIN$$'))
  )
  ,pivot_header as
  (
    select 
      * 
    from (
            select 
               1 ord
              ,'Variation Type'                        var_type
              ,'Owner'                                 owner
              ,'Object Type'                           object_type
              ,'Object Name'                           object_name
              ,'Sub-object (partition) Name'           subobject_name
              ,'Tablespace'                            tablespace_name
              ,'Current Objects Size'                  current_size_mb
              ,case level
                 when 1 then 'Today'
                 else 'D-' || to_char(level-1)
               end                                      snap_day
              ,(level - 1)                              day_offset
            from dual
            connect by level <= (&past_days+1)
        )
    pivot (
      max(day_offset) as "Date"
      for snap_day in ('Today' as "Today" ,'D-1'  as "D-1" ,'D-2'  as "D-2" ,'D-3'  as "D-3" ,'D-4'  as "D-4" ,'D-5'  as "D-5" ,'D-6'  as "D-6" ,'D-7'  as "D-7" ,'D-8'  as "D-8" ,'D-9'  as "D-9"
                      , 'D-10' as "D-10",'D-11' as "D-11",'D-12' as "D-12",'D-13' as "D-13",'D-14' as "D-14",'D-15' as "D-15",'D-16' as "D-16",'D-17' as "D-17",'D-18' as "D-18",'D-19' as "D-19"
                      , 'D-20' as "D-20",'D-21' as "D-21",'D-22' as "D-22",'D-23' as "D-23",'D-24' as "D-24",'D-25' as "D-25",'D-26' as "D-26",'D-27' as "D-27",'D-28' as "D-28",'D-29' as "D-29"
                      , 'D-30' as "D-30"
                      )
          )  
  )
  ,pivot_size as
  (
    select
      *
    from 
      (select 2 ord, 'Absolute Variation' var_type , owner    , object_type                 , object_name        ,subobject_name
                     ,tablespace_name              , snap_day , to_char(current_size_mb) cs , size_variation_mb 
       from segment_pre_pivot)
    pivot (
      max(size_variation_mb) as "Variation"
      for snap_day in ('Today' as "Today" ,'D-1'  as "D-1" ,'D-2'  as "D-2" ,'D-3'  as "D-3" ,'D-4'  as "D-4" ,'D-5'  as "D-5" ,'D-6'  as "D-6" ,'D-7'  as "D-7" ,'D-8'  as "D-8" ,'D-9'  as "D-9"
                      , 'D-10' as "D-10",'D-11' as "D-11",'D-12' as "D-12",'D-13' as "D-13",'D-14' as "D-14",'D-15' as "D-15",'D-16' as "D-16",'D-17' as "D-17",'D-18' as "D-18",'D-19' as "D-19"
                      , 'D-20' as "D-20",'D-21' as "D-21",'D-22' as "D-22",'D-23' as "D-23",'D-24' as "D-24",'D-25' as "D-25",'D-26' as "D-26",'D-27' as "D-27",'D-28' as "D-28",'D-29' as "D-29"
                      , 'D-30' as "D-30"
                      )
      )  )
  ,pivot_pct as
  (
    select
      *
    from 
      (select 3 ord ,'% of current size' var_type  , owner    , object_type                 , object_name                 ,subobject_name
                    ,tablespace_name               , snap_day , to_char(current_size_mb) cs , pct_of_current_size_variation 
       from segment_pre_pivot)
    pivot (
      max(pct_of_current_size_variation) as "Pct"
      for snap_day in ('Today' as "Today" ,'D-1'  as "D-1" ,'D-2'  as "D-2" ,'D-3'  as "D-3" ,'D-4'  as "D-4" ,'D-5'  as "D-5" ,'D-6'  as "D-6" ,'D-7'  as "D-7" ,'D-8'  as "D-8" ,'D-9'  as "D-9"
                      , 'D-10' as "D-10",'D-11' as "D-11",'D-12' as "D-12",'D-13' as "D-13",'D-14' as "D-14",'D-15' as "D-15",'D-16' as "D-16",'D-17' as "D-17",'D-18' as "D-18",'D-19' as "D-19"
                      , 'D-20' as "D-20",'D-21' as "D-21",'D-22' as "D-22",'D-23' as "D-23",'D-24' as "D-24",'D-25' as "D-25",'D-26' as "D-26",'D-27' as "D-27",'D-28' as "D-28",'D-29' as "D-29"
                      , 'D-30' as "D-30"
                      )
          )
  )
  ,result as
  (
      select -- Gets the headers, converting offset into DATE 
         ord
        ,VAR_TYPE         ,OWNER            ,OBJECT_TYPE,      OBJECT_NAME        
        ,SUBOBJECT_NAME   ,TABLESPACE_NAME  ,CURRENT_SIZE_MB
        ,'Variation over the past &past_days days (MB)' PAST_VAR
        ,to_char(trunc(sysdate)- "Today_Date",'dd/mm/yyyy') TODAY
        ,to_char(trunc(sysdate)- "D-1_Date" ,'dd/mm/yyyy') D1  ,to_char(trunc(sysdate)- "D-2_Date" ,'dd/mm/yyyy')D2  ,to_char(trunc(sysdate)- "D-3_Date" ,'dd/mm/yyyy') D3
        ,to_char(trunc(sysdate)- "D-4_Date" ,'dd/mm/yyyy') D4  ,to_char(trunc(sysdate)- "D-5_Date" ,'dd/mm/yyyy')D5  ,to_char(trunc(sysdate)- "D-6_Date" ,'dd/mm/yyyy') D6
        ,to_char(trunc(sysdate)- "D-7_Date" ,'dd/mm/yyyy') D7  ,to_char(trunc(sysdate)- "D-8_Date" ,'dd/mm/yyyy')D8  ,to_char(trunc(sysdate)- "D-9_Date" ,'dd/mm/yyyy') D9
        ,to_char(trunc(sysdate)- "D-10_Date",'dd/mm/yyyy') D10
        ,to_char(trunc(sysdate)- "D-11_Date",'dd/mm/yyyy') D11 ,to_char(trunc(sysdate)- "D-12_Date",'dd/mm/yyyy')D12 ,to_char(trunc(sysdate)- "D-13_Date",'dd/mm/yyyy') D13
        ,to_char(trunc(sysdate)- "D-14_Date",'dd/mm/yyyy') D14 ,to_char(trunc(sysdate)- "D-15_Date",'dd/mm/yyyy')D15 ,to_char(trunc(sysdate)- "D-16_Date",'dd/mm/yyyy') D16
        ,to_char(trunc(sysdate)- "D-17_Date",'dd/mm/yyyy') D17 ,to_char(trunc(sysdate)- "D-18_Date",'dd/mm/yyyy')D18 ,to_char(trunc(sysdate)- "D-19_Date",'dd/mm/yyyy') D19
        ,to_char(trunc(sysdate)- "D-20_Date",'dd/mm/yyyy') D20
        ,to_char(trunc(sysdate)- "D-21_Date",'dd/mm/yyyy') D21 ,to_char(trunc(sysdate)- "D-22_Date",'dd/mm/yyyy')D22 ,to_char(trunc(sysdate)- "D-23_Date",'dd/mm/yyyy') D23
        ,to_char(trunc(sysdate)- "D-24_Date",'dd/mm/yyyy') D24 ,to_char(trunc(sysdate)- "D-25_Date",'dd/mm/yyyy')D25 ,to_char(trunc(sysdate)- "D-26_Date",'dd/mm/yyyy') D26
        ,to_char(trunc(sysdate)- "D-27_Date",'dd/mm/yyyy') D27 ,to_char(trunc(sysdate)- "D-28_Date",'dd/mm/yyyy')D28 ,to_char(trunc(sysdate)- "D-29_Date",'dd/mm/yyyy') D29
        ,to_char(trunc(sysdate)- "D-30_Date",'dd/mm/yyyy') D30  
      from pivot_header
      where '&output_header' = 'Y'
union select -- Get lines with absolute growth values 
         ord
        ,VAR_TYPE         ,OWNER            ,OBJECT_TYPE,      OBJECT_NAME        
        ,SUBOBJECT_NAME   ,TABLESPACE_NAME  ,cs
        ,to_char (nvl("Today_Variation",0) + nvl("D-1_Variation" ,0)+ nvl("D-2_Variation" ,0)  + nvl("D-3_Variation" ,0) + nvl("D-4_Variation"   ,0) + nvl("D-5_Variation" ,0)    
                                           + nvl("D-6_Variation" ,0) + nvl("D-7_Variation" ,0) + nvl("D-8_Variation" ,0) + nvl("D-9_Variation" ,0) + nvl("D-10_Variation",0)
                                           + nvl("D-11_Variation",0) + nvl("D-12_Variation",0) + nvl("D-13_Variation",0) + nvl("D-14_Variation",0) + nvl("D-15_Variation",0)
                                           + nvl("D-16_Variation",0) + nvl("D-17_Variation",0) + nvl("D-18_Variation",0) + nvl("D-19_Variation",0) + nvl("D-20_Variation",0)
                                           + nvl("D-21_Variation",0) + nvl("D-22_Variation",0) + nvl("D-23_Variation",0) + nvl("D-24_Variation",0) + nvl("D-25_Variation",0)
                                           + nvl("D-26_Variation",0) + nvl("D-27_Variation",0) + nvl("D-28_Variation",0) + nvl("D-29_Variation",0) + nvl("D-30_Variation",0))
        ,to_char("Today_Variation")
        ,to_char("D-1_Variation") ,to_char("D-2_Variation") ,to_char("D-3_Variation")
        ,to_char("D-4_Variation") ,to_char("D-5_Variation") ,to_char("D-6_Variation")
        ,to_char("D-7_Variation") ,to_char("D-8_Variation") ,to_char("D-9_Variation")
        ,to_char("D-10_Variation")
        ,to_char("D-11_Variation"),to_char("D-12_Variation"),to_char("D-13_Variation")
        ,to_char("D-14_Variation"),to_char("D-15_Variation"),to_char("D-16_Variation")
        ,to_char("D-17_Variation"),to_char("D-18_Variation"),to_char("D-19_Variation")
        ,to_char("D-20_Variation")
        ,to_char("D-21_Variation"),to_char("D-22_Variation"),to_char("D-23_Variation")
        ,to_char("D-24_Variation"),to_char("D-25_Variation"),to_char("D-26_Variation")
        ,to_char("D-27_Variation"),to_char("D-28_Variation"),to_char("D-29_Variation")
        ,to_char("D-30_Variation")
      from pivot_size
      where '&output_absolute' = 'Y'
union select  -- Get lines with percentages of variation) 
         ord
        ,VAR_TYPE         ,OWNER            ,OBJECT_TYPE,      OBJECT_NAME        
        ,SUBOBJECT_NAME   ,TABLESPACE_NAME  ,cs
        ,to_char (nvl("Today_Pct",0) + nvl("D-1_Pct" ,0) + nvl("D-2_Pct" ,0) + nvl("D-3_Pct" ,0) + nvl("D-4_Pct" ,0) + nvl("D-5_Pct" ,0)
                                     + nvl("D-6_Pct" ,0) + nvl("D-7_Pct" ,0) + nvl("D-8_Pct" ,0) + nvl("D-9_Pct" ,0) + nvl("D-10_Pct",0)
                                     + nvl("D-11_Pct",0) + nvl("D-12_Pct",0) + nvl("D-13_Pct",0) + nvl("D-14_Pct",0) + nvl("D-15_Pct",0)
                                     + nvl("D-16_Pct",0) + nvl("D-17_Pct",0) + nvl("D-18_Pct",0) + nvl("D-19_Pct",0) + nvl("D-20_Pct",0)
                                     + nvl("D-21_Pct",0) + nvl("D-22_Pct",0) + nvl("D-23_Pct",0) + nvl("D-24_Pct",0) + nvl("D-25_Pct",0)
                                     + nvl("D-26_Pct",0) + nvl("D-27_Pct",0) + nvl("D-28_Pct",0) + nvl("D-29_Pct",0) + nvl("D-30_Pct",0))
        ,to_char("Today_Pct")
        ,to_char("D-1_Pct") ,to_char("D-2_Pct") ,to_char("D-3_Pct")
        ,to_char("D-4_Pct") ,to_char("D-5_Pct") ,to_char("D-6_Pct")
        ,to_char("D-7_Pct") ,to_char("D-8_Pct") ,to_char("D-9_Pct")
        ,to_char("D-10_Pct")
        ,to_char("D-11_Pct"),to_char("D-12_Pct"),to_char("D-13_Pct")
        ,to_char("D-14_Pct"),to_char("D-15_Pct"),to_char("D-16_Pct")
        ,to_char("D-17_Pct"),to_char("D-18_Pct"),to_char("D-19_Pct")
        ,to_char("D-20_Pct")
        ,to_char("D-21_Pct"),to_char("D-22_Pct"),to_char("D-23_Pct")
        ,to_char("D-24_Pct"),to_char("D-25_Pct"),to_char("D-26_Pct")
        ,to_char("D-27_Pct"),to_char("D-28_Pct"),to_char("D-29_Pct")
        ,to_char("D-30_Pct")
      from pivot_pct
      where '&output_pct' = 'Y'
union select 
         4
        ,'No Historical Information'         ,OWNER            ,SEGMENT_TYPE,      SEGMENT_NAME        
        ,PARTITION_NAME    ,TABLESPACE_NAME  ,to_char(bytes/1024/1024)
        ,'N/A'
        ,'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A' 
        ,'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A' 
        ,'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A'
      from 
         dba_segments 
      where segment_name not in (select 
                                  object_name 
                                from 
                                  dba_hist_seg_stat_obj
                                ) 
      and owner in (&schemas_list) 
      and segment_name like '&objects_name'
      and segment_name not like 'BIN$%'
      and segment_subtype != 'SECUREFILE'
      and nvl(partition_name,'$$MAIN$$') like '&subobjects_name'
      and '&output_missing' = 'Y'
order by 1,2,3,4,5 
  )
--select ord
--   var_type        
--  ,owner           ,object_type     ,object_name
--  ,subobject_name  ,tablespace_name ,CURRENT_SIZE_MB
--  ,TODAY
--from   result
select * from result  ;

