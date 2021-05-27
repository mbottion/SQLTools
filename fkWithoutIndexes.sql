column status            format a10
column table_name        format a80
column fk_name           format a80
column fk_columns        format a80
column index_name        format a80
column index_columns     format a80
column owner             format a10
column tab_part_columns  format a80 trunc
column ind_part_columns  format a80 trunc

define schemas_list="'BNA','TEC','LIQ1','LIQ2','LIQF1','LIQF2','SYN1','ACE1','TGP'"
define tab_name="case when '&1 is null then '%' else upper('&1') end"

select
   tab.owner
  ,case
     when ind.table_name is null then
       'unindexed'
     else
        'indexed'
    end as status
   ,tab.table_name      as table_name
   ,tab.partitioned     as tab_part
   ,tab.part_columns    as tab_part_columns
   ,tab.constraint_name as fk_name
   ,tab.fk_columns      as fk_columns
   ,ind.index_name      as index_name
   ,ind.partitioned     as ind_part
   ,ind.part_columns    as ind_part_columns
   ,ind.index_columns   as index_columns
from (
       select
          a.owner
         ,a.table_name
         ,tab.partitioned
         ,pkc.part_columns
         ,a.constraint_name
         ,listagg(a.column_name, ',') within group (order by a.position) fk_columns
       from
         dba_cons_columns a,
         dba_constraints b
       join 
         dba_tables tab on (tab.owner=b.owner and tab.table_name = b.table_name)
       left outer join 
         (select
             owner
            ,name
            ,listagg(column_name, ',') within group (order by column_position) part_columns
          from
            dba_part_key_columns
           where
             object_type = 'TABLE'
             and owner in (&&schemas_list)
           group by
             owner
             ,name ) pkc on (    tab.owner=b.owner 
                             and tab.table_name = pkc.name)
       where
           a.constraint_name = b.constraint_name
       and b.constraint_type = 'R'
       and a.owner in (&&schemas_list) and a.table_name like '&tab_name'
       and a.owner = b.owner
      group by
         a.owner
        ,a.table_name
        ,a.constraint_name
        ,tab.partitioned
        ,pkc.part_columns
     ) tab
    ,(
       select
          c.table_owner
         ,c.table_name
         ,c.index_name
         ,ind.partitioned
         ,pkc.part_columns
         ,listagg(c.column_name, ',') within group (order by c.column_position) index_columns
       from
         dba_ind_columns c
       join 
         dba_indexes ind on (ind.owner=c.index_owner and ind.index_name = c.index_name)
       left outer join 
         (select
             owner
            ,name
            ,listagg(column_name, ',') within group (order by column_position) part_columns
          from
            dba_part_key_columns
           where
             object_type = 'INDEX'
             and owner in (&&schemas_list)
           group by
             owner
             ,name ) pkc on (    ind.owner=ind.owner 
                             and ind.index_name = pkc.name)
       where
         c.index_owner in (&&schemas_list) and c.table_name like '&tab_name'
       group by
          c.table_owner
         ,c.table_name
         ,c.index_name
         ,ind.partitioned
         ,pkc.part_columns
    ) ind
where
      tab.table_name = ind.table_name(+)
  and ind.index_columns(+) like tab.fk_columns || '%'
order by
   1,2 desc, 3
/
