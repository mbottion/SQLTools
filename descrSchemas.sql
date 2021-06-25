define P1="&1"
define P2="&2"
define P3="&3"
define P4="&4"
define P5="&5"

set feedback off
set serveroutput on
begin
  if (upper('&P1') in ('USAGE','HELP','-?','-H'))
  then
    raise_application_error(-20000,'
+---------------------------------------------------------------------------------------
| Usage:
|    descrSchemas.sql 
|   
|
|   Parameters :
|       start    : Analysis start date (dd/mm/yyyy [hh24:mi:ss])      - Default : Noon (Today or yesterday)
|       end      : Analysis end date   (dd/mm/yyyy [hh24:mi:ss])      - Default : now
|       mode     : Groups on start_engine or end engine date (IN/OUT) - Defult  : IN (Injection Rate)
|       interval : Interval wideness (in seconds)                     - Default : 3600
|       engine   : Engine name                                        - Default : %
|       
+---------------------------------------------------------------------------------------
       ');
  end if ;
end ;
/
set pause off
set feed off
col db_name new_value db_name
set termout off

select
  name db_name
from 
  v$database ;

set term off


set linesize  132
set pagesize  120
set trimspool on
set newpage 2
spool verif_&db_name..lst

rem ttitle LEFT  "FAST: Migration ves ORACLE 10g" -
rem        RIGHT "Base de données : &db_name" -
rem        SKIP1 -
rem        LEFT  "Liste de vérification des schémas applicatifs"
rem btitle RIGHT "Informations générales (database, instance, liste des schémas)"

prompt
prompt
prompt
prompt
prompt .                                               F A S T
prompt
prompt
prompt
prompt .                             M i g r a t i o n   v e r s   O R A C L E   1 0 g 
prompt
prompt
prompt .                             =================================================
prompt
prompt
prompt .                             Vérification des schémas avant et après migration
prompt
prompt
prompt .                             =================================================
prompt

prompt Base de données
prompt ===============

select
   name                                       "Nom"
  ,to_char (created,'dd/mm/yyyy hh24:mi:ss')  "Création"
  ,log_mode                                   "Log mode"
  ,db_unique_name                             "Nom unique"
from
  v$database
/

prompt
prompt Instance
prompt ========

select
   INSTANCE_NAME                              "Nom"
  ,HOST_NAME                                  "Hôte"
from
  v$instance
/

prompt
prompt Liste des schémas
prompt =================

column USERNAME             format a25 heading "Nom"           
column ACCOUNT_STATUS       format a10 heading "Status"        
column DEFAULT_TABLESPACE   format a20 heading "Default TBS"   
column TEMPORARY_TABLESPACE format a20 heading "Temporary TBS" 
column DAT                  format a20 heading "Création"      



select
   USERNAME                                    
  ,ACCOUNT_STATUS                              
  ,DEFAULT_TABLESPACE                          
  ,TEMPORARY_TABLESPACE                        
  ,to_char (created,'dd/mm/yyyy hh24:mi:ss') dat
from
  dba_users 
where 
  username like '%FAST%' or username like '%AQ%'
order by
  username
/

set newpage 0
ttitle LEFT  "FAST: Migration ves ORACLE 10g" -
       RIGHT "Base de données : &db_name" -
       SKIP1 -
       LEFT  "Liste de vérification des schémas applicatifs" -
       SKIP 2
       
btitle SKIP2 - 
       RIGHT "Liste des objets"

clear columns

column OWNER          format a30 heading "Schéma"    
column OBJECT_TYPE    format a20 heading "Type"
column OBJECT_NAME    format a30 heading "Nom objet"
column SUBOBJECT_NAME format a30 heading "Sous-objet"
column STATUS         format a10 heading "Status"
column TEMPORARY      format a5  heading "Temp?"

break on OWNER skip PAGE on object_type skip 1 on report

compute count LABEL "** Nbre/Type"   of OBJECT_NAME on OBJECT_TYPE 
compute count LABEL "** Nbre/Schéma" of OBJECT_NAME on OWNER 
compute count LABEL "** Nbre total"  of OBJECT_NAME on REPORT 

select
   OWNER
  ,OBJECT_TYPE
  ,OBJECT_NAME
  ,SUBOBJECT_NAME
  ,STATUS
  ,TEMPORARY
from
  dba_objects
where
  owner like 'FAST%'
order by
   owner
  ,object_type
  ,object_name
  ,subobject_name
/

btitle SKIP2 - 
       RIGHT "Liste des tables et colonnes"

clear columns
clear break
clear compute

break on OWNER skip PAGE on table_name skip 1 on report

column OWNER          format a6  trunc heading "Schéma"
column TABLE_NAME     format a30  trunc heading "Table"
column COLUMN_NAME    format a30  trunc heading "Colonne"
column DATA_TYPE      format a15  trunc heading "Type"
column DATA_NULL      format a9   trunc heading "Null?"
column DATA_DEFAULT   format a37  trunc heading "Default"

select
   replace(replace(OWNER,'FAST',''),'ADMIN','') OWNER
  ,TABLE_NAME
  ,COLUMN_NAME
  ,rpad(DATA_TYPE,9) ||
        decode (data_type
               ,'DATE',''
               ,'LONG',''
               ,'LONGRAW',''
               ,'BLOB',''
               ,'NUMBER',decode(nvl(data_precision,0),0,'','(')
               ,'(') ||
        decode (data_type
               ,'LONG',''
               ,'LONGRAW',''
               ,'BLOB',''
               ,'NUMBER',data_precision
               ,'DATE','','LONG','','LONG RAW',''
               ,data_length ) ||
        decode (data_type
               ,'NUMBER',decode(nvl(data_scale,0),0,'',','||data_scale)
               ,'') ||
        decode (data_type
               ,'DATE',''
               ,'LONG',''
               ,'LONGRAW',''
               ,'BLOB',''
               ,'NUMBER',decode(nvl(data_precision,0),0,'',')')
               ,')') data_type
  ,decode (NULLABLE,'N',' NOT NULL') DATA_NULL
  ,DATA_DEFAULT
from
  dba_tab_columns
where
  owner like 'FAST%'
order by
   owner
  ,table_name
  ,column_id
/

btitle SKIP2 - 
       RIGHT "Liste des indexes et colonnes indexées"

clear columns
clear break
clear compute

break on OWNER skip PAGE on table_name on index_name skip 1 on report

column OWNER           format a6   trunc heading "Schéma"
column TABLE_NAME      format a30  trunc heading "Table"
column INDEX_NAME      format a30  trunc heading "Index"
column COLUMN_POSITION format 999  trunc heading "Pos"
column COLUMN_NAME     format a30  trunc heading "Colonne"
column DESCEND         format a15  trunc heading "Sens"

select
   replace(replace(INDEX_OWNER,'FAST',''),'ADMIN','') OWNER
  ,table_name
  ,index_name
  ,column_position
  ,column_name
  ,descend
from
  dba_ind_columns
where
  index_owner like 'FAST%'
order by
   index_owner
  ,table_name
  ,index_name
  ,column_position
/

set feedback on
create or replace function getCond (p_owner           in varchar2
                                   ,p_table_name      in varchar2
                                   ,p_constraint_name in varchar2
                                   ) return varchar2 as
  tmp varchar2(10000) ;
begin
  select 
    search_condition
  into
    tmp
  from
    all_constraints
  where
        owner           = p_owner
    and table_name      = p_table_name
    and constraint_name = p_constraint_name ;
  return(substr(tmp,1,50)) ;
exception
  when others then
    return ('???') ;
end ;
/

set feedback on
show err



btitle SKIP2 - 
       RIGHT "Liste et état des contraintes"

clear columns
clear break
clear compute

break on OWNER skip PAGE on table_name  skip 2 on constraint_type on constraint_name on status on report

column OWNER           format a6   trunc heading "Schéma"
column TABLE_NAME      format a30  trunc heading "Table"
column CONSTRAINT_NAME format a30  trunc heading "Contrainte"
column CONDITIONS      format a60  trunc heading "Détail + Del Rule / Deferable / Defered / Validated"
column ORD             noprint
column ORD2            noprint

select
   replace(replace(OWNER,'FAST',''),'ADMIN','') OWNER
  ,TABLE_NAME
  ,CONSTRAINT_TYPE
  ,CONSTRAINT_NAME
  ,decode(constraint_type
         ,'C',getCond(OWNER,TABLE_NAME,CONSTRAINT_NAME)
         ,'R','Ref C : ' || R_OWNER || '.' || R_CONSTRAINT_NAME 
         ,'P','Index : ' || INDEX_OWNER || '.' || INDEX_NAME 
         ,'U','Index : ' || INDEX_OWNER || '.' || INDEX_NAME 
         ,'Type non prévu: ' || constraint_type
         ) CONDITIONS
  ,10 ORD
  ,CONSTRAINT_NAME ORD2
from
  dba_constraints
where
  owner like 'FAST%'
UNION
select
   replace(replace(OWNER,'FAST',''),'ADMIN','') OWNER
  ,TABLE_NAME
  ,CONSTRAINT_TYPE
  ,'---> ' || STATUS
  ,rpad(nvl(DELETE_RULE,' '),10) || '/ ' ||
   rpad(nvl(DEFERRABLE ,' '),15) || '/ ' ||
   rpad(nvl(DEFERRED   ,' '),10) || '/ ' ||
   rpad(nvl(VALIDATED  ,' '),10) || 
   ''
  ,20 ORD
  ,CONSTRAINT_NAME
from
  dba_constraints
where
  owner like 'FAST%'
order by
   1
  ,2
  ,3
  ,7
  ,6
/

set feedback off
drop function getCond ;
set feedback on
 
btitle SKIP2 - 
       RIGHT "Liste des séquences"

clear columns
clear break
clear compute

break on OWNER skip 2

column OWNER           format a6                trunc heading "Schéma"
column SEQUENCE_NAME   format a30               trunc heading "Nom"
column MIN_VALUE       format 99                trunc heading "Min"
column MAX_VALUE       format 999G999G999G999G999G999G999G999G999  trunc heading "Max"
column INCREMENT_BY    format 99                trunc heading "Incr."
column CYCLE_FLAG      format a10               trunc heading "Cycle"
column ORDER_FLAG      format a10               trunc heading "Order"
column CACHE_SIZE      format 9999999           trunc heading "Cache"
column LAST_NUMBER     format 999G999G999 trunc heading "Dernier"

select
   replace(replace(SEQUENCE_OWNER,'FAST',''),'ADMIN','') OWNER
  ,SEQUENCE_NAME
  ,MIN_VALUE
  ,MAX_VALUE
  ,INCREMENT_BY
  ,decode(CYCLE_FLAG,'Y','CYCLE','N','NO CYCLE',CYCLE_FLAG) CYCLE_FLAG 
  ,decode(ORDER_FLAG,'Y','ORDER','N','NO ORDER',ORDER_FLAG) ORDER_FLAG 
  ,CACHE_SIZE
  ,LAST_NUMBER
from
  dba_sequences
where
  sequence_owner like 'FAST%'
order by
   sequence_owner
  ,sequence_name
/

spool off

set linesize 80
clear columns
clear breaks
set term on
set pagesize 23
set pause off
ttitle off
btitle off

rem exit
