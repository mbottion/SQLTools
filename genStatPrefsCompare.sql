define P1="&1"
define P2="&2"
define P3="&3"
define P4="&4"
define P5="&5"

--define P1="DBA2AP"
--define P2="%"
--define P3="DEGREE"
set feedback off
set serveroutput on
set verify off
declare
 dummy number ;
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    genParamCompare.sql [schema] [table] [pref]
|   
|      Generates a SQL SCript to by run on another DATABASE to compare init parameters :
|
|   Parameters :
|       mode     : sceham name ("%" allowed)
|       table    : table_name ("%" allowed)
|       pref     : pref name ("%" allowed)
|       
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/

-- -----------------------------------------------------------------
-- Parameters (use P1 -- PN, to ease script test in SQL*Dev)
-- -----------------------------------------------------------------
--
--  Comparison mode
--

define schemaName="case when '&P1' is null then '%' else upper('&P1') end"
define tableName ="case when '&P2' is null then '%' else upper('&P2') end"
define prefName ="case when '&P3' is null then '%' else upper('&P3') end"


define liste1="APPROXIMATE_NDV_ALGORITHM,AUTO_STAT_EXTENSIONS,AUTO_TASK_STATUS,AUTO_TASK_MAX_RUN_TIME,AUTO_TASK_INTERVAL,CASCADE,CONCURRENT"
define liste2="DEGREE,ESTIMATE_PERCENT,GLOBAL_TEMP_TABLE_STATS,GRANULARITY,INCREMENTAL,INCREMENTAL_STALENESS,INCREMENTAL_LEVEL"
define liste3="METHOD_OPT,NO_INVALIDATE,OPTIONS,PREFERENCE_OVERRIDES_PARAMETER,PUBLISH,STALE_PERCENT,STAT_CATEGORY,TABLE_CACHED_BLOCKS"

set lines 500 heading off feedback off pagesize 2000
column ord format 999 noprint
column a format a400 
column b format a400 newline
set recsep off

with
  function get_or_comp_pref(s in varchar2,t in varchar2,p in varchar2,old_value in varchar2 default null) return varchar2 is
    v varchar2(100) ;
  begin
    begin
      v := dbms_stats.get_prefs(p,s,t) ; 
    exception when others then
      if    (sqlcode =-20001 ) then v := 'N/A' ;
      elsif (sqlcode =-20000 ) then v := 'Non Existent Table' ;
      else                          raise ;
      end if ;
    end ;
    if old_value is null then return (nvl(v,'$Null$')) ;
    else
      if old_value <> v then return ('DIFFERENT') ;
      else                   return ('SAME');
      end if ;
    end if ;
  end ;
select
         'Prompt                                                                          ' || chr(10) ||
         'Prompt     Running preference comparison ....                                   ' || chr(10) ||
         'Prompt     ==================================                                   ' || chr(10) ||
         'Prompt                                                                          ' || chr(10) ||
         'Prompt     If a lot of tables are selected, this script may run for             ' || chr(10) ||
         'Prompt  several hours, please be patient ....                                   ' || chr(10) ||
         'Prompt                                                                          ' || chr(10) ||
         'col owner                    format a10                                         ' || chr(10) ||
         'col table_name               format a30                                         ' || chr(10) ||
         'col value                    format a50                                         ' || chr(10) ||
         'col value_' || name || '     format a50                                         ' || chr(10) ||
         'with                                                                            ' || chr(10) ||
         '  function get_or_comp_pref(s in varchar2, t in varchar2                        ' || chr(10) ||
         '                           ,p in varchar2, old_value in varchar2 default null)  ' || chr(10) ||
         '  return varchar2 is                                                            ' || chr(10) ||
         '    v varchar2(100) ;                                                           ' || chr(10) ||
         '  begin                                                                         ' || chr(10) ||
         '    begin                                                                       ' || chr(10) ||
         '      v := dbms_stats.get_prefs(p,s,t) ;                                        ' || chr(10) ||
         '    exception when others then                                                  ' || chr(10) ||
         '      if   (sqlcode =-20001 )  then v := ''N/A'' ;                              ' || chr(10) ||
         '      elsif (sqlcode =-20000 ) then v := ''Non Existant Table'' ;               ' || chr(10) ||
         '      else                          raise ;                                     ' || chr(10) ||
         '      end if ;                                                                  ' || chr(10) ||
         '    end ;                                                                       ' || chr(10) ||
         '    if old_value is null then return (nvl(v,''NULL'')) ;                        ' || chr(10) ||
         '    else                                                                        ' || chr(10) ||
         '      if old_value <> v then return (''DIFFERENT'') ;                           ' || chr(10) ||
         '      else                   return (''SAME'');                                 ' || chr(10) ||
         '      end if ;                                                                  ' || chr(10) ||
         '    end if ;                                                                    ' || chr(10) ||
         '  end ;                                                                         ' || chr(10) ||
         '' a
  ,'-- ------------------------------------------------' a
  ,5    ord
  ,null ord
  ,null ord
  ,null ord
from v$database
UNION
select 
   '      select null Owner  ,null table_name  , null pref_name                  , null VALUE                                                                    , null VALUE_' || name || ' from dual where 1=2' a
  ,'UNION select null Owner  ,null table_name  , null pref_name                  , null VALUE                                                                    , null VALUE_' || name || ' from dual where 1=2' a
  ,10   ord 
  ,null ord
  ,null ord
  ,null ord
from v$database
UNION
select
   'UNION select owner       ,table_name       , '''||pref_name||''''||
      '  , get_or_comp_pref('''||owner||''','''||table_name||''','''||pref_name||'''),'''||pref_value|| '''    '||
      '    from dba_tables where owner=''' || owner ||''' and table_name=''' || table_name || '''' ||
      ' and get_or_comp_pref('''||owner||''','''||table_name||''','''||pref_name||''','''||pref_value||''') = ''DIFFERENT''' ||
      ''
  ,'UNION select '''||owner||'''       ,'''||table_name||'''       , '''||pref_name||''''||
     ',''*** Non Existant TABLE ***'','''|| pref_value || ''' from dual ' || 
     ' where not exists (select 1 from dba_tables where owner=''' || owner||''' and table_name=''' ||table_name || ''')' ||
     ''
  ,20
  ,owner
  ,table_name
  ,pref_name
from
  (select
    owner
    ,table_name
    ,pref_name
    ,get_or_comp_pref(owner,table_name,pref_name) pref_value
  from
    dba_tables
  join (SELECT  
          regexp_substr('&liste1,&liste2,&liste3', '(,)?([^,]*)(,)?', 1, level, 'i', 2) pref_name
        FROM    
          dual 
        connect BY 
          level <= regexp_count ('&liste1,&liste2,&liste3', ',') + 1) on (1=1) 
  where 
      owner like &schemaName
--    and owner not in ('ACE1','BNA','CTDBCT','INF','LIQ1','LIQ2','LIQF1','LIQF2','MES','SYN1','TEC','TGP','WIB')
--    and owner not in ('DBA2AP','RDACCENTURE')
    and owner not in (select username from dba_users where oracle_maintained='Y')
--    and table_name like &tableName
    and table_name in ('RES_ANALYSE_RESSOURCE','RES_PERIODE_MANQUANTE','RES_RESSOURCE_RETENUE','PER_RES_BASE','PER_RES_BASE_ANALYSE','GPI_PRESTATION','GPI_DROIT','GPI_DROIT_RESANALYSE','PER_MAINTIEN_DROIT')
    and pref_name like &prefName)
union
select 
  'order by 2,1'
  ,'/'
  ,40 ord 
  ,null
  ,null
  ,null
from dual
order by 3,4,5,6 
/
exit
