define tbs=TBS_BNA0PRD_BNA_ACTIVE
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--     Purge de la corbeille, sinon, ça fait plein d'erreurs, et çà prend du
--  temps pour rien
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
purge RECYCLEBIN ;
purge dba_recyclebin ;
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--   Repérage des segments les plus gros qu'il vaut mieux déplacer avant
-- ou avec des exécutions spécifiques du script de déplacement.
--
--   Adapter le script de lancement en conséquence
-- 
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
SELECT
  segment_name
 ,segment_type
 ,partition_name
 ,sum(bytes) / 1024 / 1024 / 1024 size_gb
FROM
  dba_segments
WHERE
  tablespace_name   = '&tbs'
  and segment_type  in ('TABLE','TABLE PARTITION','LOB', 'LOB PARTITION')
GROUP BY
  segment_name
 ,segment_type
 ,partition_name
ORDER BY 4 desc
;

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--    Création des tablespaces UNDO en BIGFILES
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
create bigfile undo tablespace BIGUNDO_1 ;
create bigfile undo tablespace BIGUNDO_2 ;

alter system set undo_tablespace=BIGUNDO_1 sid='tst18c1'  scope=both ; -- A lancer sur la serveur 1
alter system set undo_tablespace=BIGUNDO_2 sid='tst18c2'  scope=both ; -- A lancer sur le serveur 2

select inst_id,value from gv$parameter where name='undo_tablespace' ;

--
--    Suppression des anciens UNDO, il est possible que çà ne passe pas
-- tout de suite, il faut attendre que toutes les transactions actives soient terminées
--
select * from dba_tablespaces where contents='UNDO' ;
drop tablespace UNDO_3 including CONTENTS and DATAFILES;
drop tablespace UNDO_4 including CONTENTS and DATAFILES;

SELECT
  *
FROM
  dba_data_files
WHERE
  tablespace_name IN (
    SELECT
      tablespace_name
    FROM
      dba_tablespaces
    WHERE
      contents   = 'UNDO'
  );

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--     Recalculer les statistiques dictionnaire et FIXED
--  sinon les selects dans DBA_EXTENTS sont très longs
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
exec dbms_stats.gather_dictionary_stats ;
exec dbms_stats.gather_fixed_objects_stats ;
EXEC DBMS_STATS.GATHER_TABLE_STATS('SYS', 'X$KTFBUE');

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--   Liste des programmes actifs, normalement, çà correspond au nombre de scripts
--  lancés en parallèle. Au début, s'assurer qu'il n'y e a pas qui restent d'une précédente 
--  execution
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
SELECT
  *
FROM
  dba_scheduler_programs
WHERE
  program_name LIKE 'REDTBS%';

--
--    Pour les supprimer
--
SELECT
  'execute dbms_scheduler.drop_program(''' || program_name || ''')'
FROM
  dba_scheduler_programs
WHERE
  program_name LIKE 'REDTBS%';

--
--        Liste des jobs en cours, on voit le nom du segment en cours pour chaque JOB
--
SELECT
  job_name
 ,program_name
 ,state
 ,start_date
 ,comments
FROM
  dba_scheduler_jobs
WHERE
  job_name LIKE 'RUN_SHELL%'
  and state != 'DISABLED';
  
--
--    Liste des JOBS terminés, plus il y a de scripts en parallèle, plus il y a de FAILED, 
--  mais c'est normal
--
SELECT
  log_date
 ,job_name
 ,status
 ,run_duration
 ,additional_info
FROM
  dba_scheduler_job_run_details
WHERE
  job_name LIKE 'RUN_SHELL%'
  and actual_start_date > to_date('11/02/2022 18:50:00')
  and status='FAILED' 
ORDER BY
  log_date DESC;  

SELECT
  status
 ,count(*)
FROM
  dba_scheduler_job_run_details
WHERE
  job_name LIKE 'RUN_SHELL%'
  and actual_start_date > to_date('11/02/2022 18:50:00')
group  BY
  status;    

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--    Génération des alter system kill pour arrêter un traitement en cours.
-- Il faut parfois s'y reprendre plusieurs fois, et/ou tuer les SQLPLUS
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
select 
   inst_id
  ,sid
  ,serial#
  ,client_info
  ,module,action
  ,'alter system kill session '''||sid||','||serial#||',@'||inst_id||''';' 
from 
  gv$session 
where 
      type='USER' 
  and module like 'reduceTablespace%'
/

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--       Query LONGOPS, les move n'apparaîssent pas ici, en revanche, pour les indexes, on peu
--  Voir où l'on en est (pas toujours)
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--

SELECT
   s.module
  ,l.elapsed_seconds
  ,l.time_remaining
  ,to_char((l.sofar/l.totalwork)*100,'990D00') || ' %' "Progress"
  ,s.action
  ,l.opname
  ,s.inst_id
  ,s.sid
  ,s.serial#
--  ,l.sql_plan_operation
--  ,l.qcsid
--  ,l.*
--  ,s.*
FROM
  gv$session_longops l
join gv$session s on (    s.inst_id = l.inst_id
                      and s.sid=l.sid
                      and s.serial# = l.serial# )
WHERE
  time_remaining > 0
order by s.module;

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--     Nombre d'objets déplacés la somme des deux peut être supérieure au nombre 
--  total car il y a des segments temporaires créés pendant le déplacement.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
select 
   tablespace_name
  ,count(*)
from 
   dba_segments
where
   tablespace_name in ('&tbs','&tbs._NEW')
--   and segment_name='RES_RESSOURCE'
group by
  tablespace_name;

--
--        Suivi de la position des segments et des extents le premier ordre
--  est plus rapide, le deuxième donne la position de chaque extent et l'espace libre.
--
--       Il est important qu'il y ait toujours un process travaillant sur le premier extend, sinon, le 
--  fichier ne peut pas diminuer
--

--
--   Optimise le select ci-dessous (19c)
--    ==> Query Against DBA_EXTENTS Performs Slowly After Upgrade to 11.2.0.3 (Doc ID 1453425.1)
--
-- alter session set "_optimizer_cartesian_enabled" = false;
-- alter session set "_smm_auto_cost_enabled" = false;
--
-- Hint dans le code
--

SELECT
  job_name
 ,program_name
 ,state
 ,start_date
 ,comments
FROM
  dba_scheduler_jobs
WHERE
  job_name LIKE 'RUN_SHELL%'
  and state != 'DISABLED';


define tbs=TBS_BNA0PRD_BNA_ACTIVE
--
--     Juste les segments dans l'ordre de déplacement
--
with
high_segs as (
                  select /*+ ALL_ROWS */
                     owner
                    ,segment_name
                    ,partition_name
                    ,segment_type
                    ,max(block_id) block_id
                    ,sum(bytes)    total_bytes
                  from
                     dba_extents
                  where
                        tablespace_name = '&tbs'
                    and segment_name like upper('%')
                    and segment_type != 'TEMPORARY'
                  group by
                     owner
                    ,segment_name
                    ,partition_name
                    ,segment_type
                  order by block_id desc
                   )
  ,running_jobs as (
                    select 
                       job_name 
                      ,regexp_replace(regexp_replace(info
                                                    ,'^([^ ]*) ([^ ]*) ([^ ]*)$'
                                                    ,'\1')
                                     ,'(TABLE|INDEX|LOB)_(SUB|)PARTITION'
                                     ,'\1 \2PARTITION') segment_type
                      ,regexp_replace(info,'^([^ ]*) ([^ ]*) ([^ ]*)$','\2') segment_name
                      ,regexp_replace(info,'^([^ ]*) ([^ ]*) ([^ ]*)$','\3') partition_name
                    from ( SELECT regexp_replace(regexp_replace(replace(comments,'Move : ','')
                                                               ,'[()]'
                                                               ,'')
                                                ,'(TABLE|INDEX|LOB) (SUB|)PARTITION'
                                                ,'\1_\2PARTITION') info
                                  ,job_name
                           FROM   dba_scheduler_jobs
                           WHERE  job_name LIKE 'RUN_SHELL%' and state != 'DISABLED')
                   )
select /*+ opt_param('_smm_auto_cost_enabled','false') */
     s.owner
    ,s.segment_name
    ,s.partition_name
    ,s.segment_type
    ,s.block_id
    ,to_char((s.block_id*8192)/1024/1024/1024,'999G990D99') position_GB
    ,to_char(s.total_bytes/1024/1024/1024,'999G990D99') seg_size_GB
    ,j.job_name Processed_by
from
  high_segs s
left join running_jobs j on (s.segment_name = j.segment_name
                             and s.segment_type = j.segment_type
                             and nvl(s.partition_name,'NA') = nvl(j.partition_name,'NA'))
--where j.job_name is not null;
--where s.segment_name like 'RES_RESSOURCE_RETE%'
--where s.segment_name like 'RES_RESSOURCE%'
--where s.segment_name like '%DEMUSAGER%'
order by block_id desc
/

--
--    Détail de l'espace avec les segments, l'espace libre, la taille du fichier et les estpaces libres
-- en dessous de l'objet et occupés au dessus
--
WITH space_usage AS (
  SELECT
    owner
   ,space_type
   ,segment_type
   ,segment_name
   ,partition_name
   ,space_start
   ,space_end
   ,space_grp
   ,CASE
      WHEN space_id IS NULL THEN LAST_VALUE( space_id IGNORE NULLS ) OVER(
        ORDER BY
          space_start DESC
      )
      ELSE space_id
    END space_id
  FROM
    (
      SELECT
        owner
       ,space_type
       ,segment_type
       ,segment_name
       ,partition_name
       ,space_start
       ,space_end
       ,space_grp
       ,CASE
          WHEN space_grp != prev_space_grp THEN space_grp || '_' || space_start
        END space_id
      FROM
        (
          SELECT
            owner
           ,space_type
           ,segment_name
           ,segment_type
           ,partition_name
           ,space_start
           ,space_end
           ,space_type || '_' || segment_name || '_' || partition_name || '_' || owner space_grp
           ,LEAD( space_type || '_' || segment_name || '_' || partition_name || '_' || owner,1,0 ) OVER(
              ORDER BY
                space_start
            ) prev_space_grp
          FROM
            (
              SELECT /*+ opt_param('_smm_auto_cost_enabled','false') */
                'Extent' space_type
               ,segment_name
               ,nvl( partition_name,'N/A' ) partition_name
               ,block_id   space_start
               ,block_id + blocks -1 space_end
               ,segment_type
               ,owner
              FROM
                dba_extents
              WHERE
                tablespace_name   = '&tbs'
              UNION
              SELECT /*+ opt_param('_smm_auto_cost_enabled','false') */
                'Free'
               ,'- - - - - - - - - - - - - - - - - '
               ,'- - - - - - - - - - - - - - - - - -'
               ,block_id
               ,block_id + blocks
               ,null
               ,'***   Free   ***'
              FROM
                dba_free_space
              WHERE
                tablespace_name   = '&tbs'
              UNION
              SELECT
                'TopBlock'
               ,' '
               ,' '
               ,blocks
               ,blocks
               ,null
               ,'*** TopBlock ***'
              FROM
                dba_data_files
              WHERE
                tablespace_name   = '&tbs'
            )
        )
    )
),grouped_space AS (
  SELECT
    owner
   ,space_type
   ,segment_name
   ,segment_type
   ,partition_name
   ,MIN( space_start ) space_start
   ,MAX( space_end ) space_end
  FROM
    space_usage
  GROUP BY
    owner
   ,space_type
   ,segment_name
   ,segment_type
   ,partition_name
   ,space_id
)
,space_map_tmp as (
SELECT
  owner
 ,segment_name
 ,segment_type
 ,partition_name
 ,( ( space_end - space_start ) * ( 8 * 1024 ) ) / 1024 / 1024 size_mb
 ,( space_start * 8 * 1024 ) / 1024 / 1024 / 1024 position_from_start_gb
 ,case space_type when 'Free' then ( ( space_end - space_start ) * ( 8 * 1024 ) ) / 1024 / 1024 else 0  end free_space_MB
 ,case space_type when 'Free' then 0 else ( ( space_end - space_start ) * ( 8 * 1024 ) ) / 1024 / 1024  end occupied_space_MB
 ,space_start
 ,space_end
 ,space_type
FROM
  grouped_space
ORDER BY
  space_start asc
)
,space_map as (
SELECT
  owner
 ,segment_name
 ,segment_type
 ,partition_name
 ,size_mb
 ,position_from_start_gb
 ,free_space_MB
 ,space_start
 ,space_end
 ,space_type
 ,sum(free_space_MB) over (order by space_start asc) free_below_mb
 ,sum(occupied_space_MB) over (order by space_start desc) occupied_above_mb
from
  space_map_tmp
)
  ,running_jobs as (
                    select 
                       job_name 
                      ,regexp_replace(regexp_replace(info
                                                    ,'^([^ ]*) ([^ ]*) ([^ ]*)$'
                                                    ,'\1')
                                     ,'(TABLE|INDEX|LOB)_(SUB|)PARTITION'
                                     ,'\1 \2PARTITION') segment_type
                      ,regexp_replace(info,'^([^ ]*) ([^ ]*) ([^ ]*)$','\2') segment_name
                      ,regexp_replace(info,'^([^ ]*) ([^ ]*) ([^ ]*)$','\3') partition_name
                    from ( SELECT regexp_replace(regexp_replace(replace(comments,'Move : ','')
                                                               ,'[()]'
                                                               ,'')
                                                ,'(TABLE|INDEX|LOB) (SUB|)PARTITION'
                                                ,'\1_\2PARTITION') info
                                  ,job_name
                           FROM   dba_scheduler_jobs
                           WHERE  job_name LIKE 'RUN_SHELL%' and state != 'DISABLED')
                   )
--SELECT  * FROM  space_usage where space_type='Free' ORDER BY  space_start DESC
--select * from running_jobs ;
SELECT
  s.owner
 ,s.segment_type 
 ,s.segment_name 
 ,s.partition_name
 ,TO_CHAR( s.size_mb,'999G999G990D09' ) size_mb
 ,TO_CHAR( s.position_from_start_gb,'999G999D09' ) position_from_start_gb
 ,TO_CHAR( s.free_below_mb/1024,'999G999G990D09' ) free_below_GB
 ,TO_CHAR( s.occupied_above_mb/1024,'999G999G990D09' ) occupied_above_GB
 ,j.job_name processed_by
 ,s.space_start
 ,s.space_end
 ,s.space_type
FROM
  space_map s
left join running_jobs j on (s.segment_name = j.segment_name
                             and s.segment_type = j.segment_type
                             and nvl(s.partition_name,'N/A') = nvl(j.partition_name,'N/A'))
--where ((( space_end - space_start ) * ( 8 * 1024 ) ) / 1024 / 1024) > 10000
--WHERE
--  segment_name like 'SYS%'
--  space_type = 'Free'
ORDER BY
  space_start desc;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--       Indexes UNUSABLE restant à reconstruire
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
SELECT
  'Unusable indexes' "Type"
 ,tablespace_name
 ,COUNT(*) "Count"
FROM
  dba_indexes
WHERE
  status   = 'UNUSABLE'
  AND tablespace_name LIKE '&tbs%'
GROUP BY
  'Unusable indexes'
 ,tablespace_name
UNION
SELECT
  'Unusable Index Partitions'
 ,tablespace_name
 ,COUNT(*)
FROM
  dba_ind_partitions
WHERE
  status   = 'UNUSABLE'
  AND tablespace_name LIKE '&tbs%'
GROUP BY
  'Unusable Index Partitions'
 ,tablespace_name
/

SELECT
  tablespace_name
 ,index_name
 ,null
FROM
  dba_indexes
WHERE
  status   = 'UNUSABLE'
  AND tablespace_name LIKE '&tbs%'
UNION
SELECT
  tablespace_name
 ,index_name
 ,partition_name
FROM
  dba_ind_partitions
WHERE
  status   = 'UNUSABLE'
  AND tablespace_name LIKE '&tbs%'
/

select 
  'alter index ' || owner || '.' || index_name || ' rebuild tablespace &tbs._NEW parallel 16 ;'
  ,'alter index ' || owner || '.' || index_name || ' parallel 1 ;'
from 
  dba_indexes
WHERE
  status   = 'UNUSABLE'
  AND tablespace_name LIKE '&tbs%'
/

--
--     Taille des fichiers, y compris les UNDO. En mode OFFLINE, les UNDO ne grossissent pas
--
col file_name format a120
col "Size GB" format a15
SELECT
  tablespace_name
 ,file_name
 ,TO_CHAR( bytes / 1024 / 1024 / 1024,'999G990D00' ) "Size GB"
FROM
  dba_data_files
WHERE
  tablespace_name IN (
    '&tbs'
   ,'&tbs._NEW'
   ,'BIGUNDO_1'
   ,'BIGUNDO_2'
  )
order by 1;
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--    Vérification des degrés de parallélisme et ordres pour les remettre à 1
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
select
  'alter ' || typ || ' ' || owner || '.' || name || ' parallel 1 /* ( Old (' || degree || ')*/ ;' "Reinit Command"
from (
  select 
     'INDEX' typ
     ,owner
     ,index_name name
     ,degree
  from dba_indexes
  where owner in ('BNA','TEC','LIQ1','LIQ2','LIQF1','LIQF2','SYN1','ACE1')
  and   index_name not like 'SYS_IL%'
  and   rtrim(ltrim(degree)) != '1' 
  UNION
  select 
     'TABLE' typ
     ,owner
     ,table_name
     ,degree
  from dba_tables
  where owner in ('BNA','TEC','LIQ1','LIQ2','LIQF1','LIQF2','SYN1','ACE1')
  and   rtrim(ltrim(degree)) != '1' )
/
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--           POur tester le mécanisme de lancement de SQL*PLus via un DBMS_SCHEDULER et le retout des erreurs
--  Le script SHELL ci après peut être utilisé comme base (a mettre dans un répertoire partagé entre toutes les
--  instances de la base
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
/* Script SHELL *************************************************************************************************
#!/bin/bash
. /home/oracle/tst18c.env
date >> /admindb/work/testSQL.log
res=$(sqlplus -s / as sysdba <<%%
whenever sqlerror exit failure
set feed off
set timing on
--spool /admindb/work/run.lst
alter session set container=bna0t18 ;
--
--  Session parameters
--
exec dbms_application_info.set_module('reduceTablespace_JOB (PID: $$)','Start') ;
exec dbms_application_info.set_action('$3') ;
exec dbms_application_info.set_client_info('Move JOB') ;
alter session set parallel_force_local=true ;
alter session set INMEMORY_QUERY=disable ;
--alter session enable parallel dml ;
alter session force  parallel dml parallel $2 ;
alter session enable parallel query ;
set feed on
--
--  Run the statement
--
declare
 e exception ;
 pragma exception_init(e,-54) ;
begin
  raise e ;
end ;
/
$1 ;
--spool off
%%
)
status=$?
echo "========= Fin operation ================" >> /admindb/work/testSQL.log
date >> /admindb/work/testSQL.log
echo "$1" >> /admindb/work/testSQL.log
echo "$res" >> /admindb/work/testSQL.log
echo "$res" | grep "^ *ORA-" | egrep -v "ORA-00604|ORA-06550" >&2
exit 2
*****************************************************************************************************************/
begin
  begin
    dbms_scheduler.drop_program('TEST_RUN') ;
  exception
    when others then null ;
  end ;
  dbms_scheduler.create_program(program_name => 'TEST_RUN'
                               ,program_type => 'EXECUTABLE'
                               ,program_action => '/admindb/work/testSQL.sh'
                               ,enabled => FALSE
                               ,comments => 'Test sqlplus from db'
                               ,number_of_arguments => 3
                               );
  DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT (program_name            => 'TEST_RUN'
                                         ,argument_position       => 1
                                         ,argument_name           => 'stmt'
                                         ,argument_type           => 'VARCHAR2'
                                         ,default_value           => 'xxx');
  DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT (program_name            => 'TEST_RUN'
                                         ,argument_position       => 2
                                         ,argument_name           => 'para'
                                         ,argument_type           => 'VARCHAR2'
                                         ,default_value           => '8');
  DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT (program_name            => 'TEST_RUN'
                                         ,argument_position       => 3
                                         ,argument_name           => 'comm'
                                         ,argument_type           => 'VARCHAR2'
                                         ,default_value           => 'Move SQLPLUS');
  dbms_scheduler.enable(name => 'TEST_RUN') ;
end;
/

select * from dba_scheduler_programs where program_name = 'TEST_RUN' ;

declare
  err_mess varchar2(2000) ;
  err number ;
  param_parallel number := 4 ;
  
  function exec_sql_in_sqlplus (c in varchar2 , ora_error_mess in out varchar2,comm in varchar2 default null) return number is
    l_job_name varchar2(100) ;
    l_job_output varchar2(4000) ;
    ora_error_code number ;
    found boolean ;
    finished boolean ;
    err number ;
  begin
    l_job_name := 'TEST_SQL_' || to_char(systimestamp,'YYYYMMDD_HH24MISS_FF') ;
    dbms_scheduler.create_job(job_name => l_job_name
                             ,program_name => 'TEST_RUN'
                             ,comments =>  comm
                             ,enabled => false
                             );
    dbms_scheduler.set_job_argument_value(l_job_name,1,c) ;
    dbms_scheduler.set_job_argument_value(l_job_name,2,to_char(param_parallel)) ;
    dbms_scheduler.set_job_argument_value(l_job_name,3,comm) ;
    dbms_scheduler.enable(l_job_name) ;
--    message ('Launched ' || l_job_name || ' (SQLPLUS JOB)'
--            ,'      |  > ',ts=>false);
    dbms_lock.sleep(1) ;
    finished := false ;
    while not finished
    loop
      begin
        select 0
        into err
        from dba_scheduler_jobs
        where job_name = l_job_name
        and   state in ('RUNNING','SCHEDULED','RETRY SCHEDULED') ;
      exception when no_data_found then 
        dbms_lock.sleep(10) ;
        finished := true ;
      end;
    end loop;
    found := false ;
    while not found
    loop
      begin
        select additional_info,error#
        into   l_job_output,err
        from   dba_scheduler_job_run_details
        where  job_name = l_job_name ;
        found := true ;
      exception when no_data_found then 
        dbms_lock.sleep(1) ;
      end ;
    end loop ;
    if (err != 0)
    then
      dbms_output.put_line(l_job_output);
      l_job_output := replace(l_job_output,'ORA-27369','XXX-27369') ;
      --l_job_output := regexp_replace(l_job_output,'^.*ORA-','ORA-',1,1,'n') ;
      l_job_output := substr(l_job_output,instr(l_job_output,'ORA-')) ;
      l_job_output := l_job_output || chr(10) ;
      ora_error_mess := substr(l_job_output,1,instr(l_job_output,chr(10))-1) ;
      ora_error_mess := replace(ora_error_mess,'\"','') ;
      if (ora_error_mess like 'ORA-%' )
      then
        ora_error_code := -to_number(substr(ora_error_mess,5,5)) ; 
      else
        ora_error_code := -666 ;
      end if ;
      return(ora_error_code) ;
    end if ;
    return (0) ;
  end ;
begin
  dbms_output.put_line('Démarrage') ;
  err := exec_sql_in_sqlplus ('select x from toto' , err_mess,'Comment') ;
  dbms_output.put_line('Fin  : ' || err) ;
  dbms_output.put_line('Mess : ' || err_mess) ;
end ;
/
set serveroutput on 
SELECT
  log_date
 ,job_name
 ,status
 ,run_duration
 ,additional_info
FROM
  dba_scheduler_job_run_details
WHERE
  job_name LIKE 'TEST_SQL%'
  and actual_start_date > to_date('08/02/2022 21:40:00')
ORDER BY
  log_date DESC;  

declare
 e exception ;
 pragma exception_init(e,-54) ;
begin
  raise e ;
end ;
/
  