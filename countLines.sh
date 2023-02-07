die()
{
  echo "ERREUR : $*

  Abandon du script à $(date)"
  exit 1
}
getStmts()
{
sqlplus -s / as sysdba <<%%
set pages 0
set feed off
set head off
set lines 2000
col l format a2000
alter session set container = $PDB;
SELECT
  'select ''' || owner || '.' || table_name ||''' TABLE_NAME '||
  ',(select ' || case 
                   when num_rows between 0         and  10000000  then ''
                   when num_rows between 10000001  and  50000000  then '/*+ PARALLEL(4) */'
                   when num_rows between 50000001  and 100000000  then '/*+ PARALLEL(8) */'
                   when num_rows between 100000001 and 500000000  then '/*+ PARALLEL(16) */'
                   when num_rows between 500000001 and 1000000000 then '/*+ PARALLEL(32) */'
                   else '/*+ PARALLEL(64) */'
                end
              ||'count(*) from ' || owner || '.' || table_name || '@BNA0PRD_18C.DBARG.PRD.ORACLEVCN.COM as of timestamp ' 
                    || 'to_timestamp(''' || to_char((systimestamp - INTERVAL '1' MINUTE),'dd/mm/yyyy hh24:mi:ss.FF9') 
                    || ''' ,''dd/mm/yyyy hh24:mi:ss.FF9'')) '
                    ||' SOURCE_ROWS' ||
  ',(select ' || case 
                   when num_rows between 0         and  10000000  then ''
                   when num_rows between 10000001  and  50000000  then '/*+ PARALLEL(4) */'
                   when num_rows between 50000001  and 100000000  then '/*+ PARALLEL(8) */'
                   when num_rows between 100000001 and 500000000  then '/*+ PARALLEL(16) */'
                   when num_rows between 500000001 and 1000000000 then '/*+ PARALLEL(32) */'
                   else '/*+ PARALLEL(64) */'
                end
              ||'count(*) from ' || owner || '.' || table_name || '                                     as of timestamp ' 
                    || 'to_timestamp(''' || to_char((systimestamp - INTERVAL '1' MINUTE),'dd/mm/yyyy hh24:mi:ss.FF9') 
                    || ''' ,''dd/mm/yyyy hh24:mi:ss.FF9'')) '
                    ||' TARGET_ROWS' ||
  ' from dual' l
FROM
  dba_tables
WHERE
  owner NOT IN (    SELECT      username    FROM      dba_users    WHERE      oracle_maintained = 'Y'  )
  and owner not in ('ACE1','SYN1','LIQ1','LIQ2','LIQF1','LIQF2','INF','RDACCENTURE','C##OGGADMIN','CTDBCT')
  and temporary = 'N'
  and external = 'NO'
  and table_name not like 'SYS_EXPORT%'
  --and num_rows < 1000000000
  --and num_rows < 1000000
  --and rownum<52
/
%%
}
genRunStmt()
{
cat >/tmp/runStmt.sh<< %EOF%
  s="\$1"
  num=\$2
  LOG_DIR=\$3
  PDB=\$4
  start_date=\$(date "+%d/%m/%Y %H:%M:%S")
  start_date_epoch=\$(date +%s)

  toRun=\$(
  echo "with compare as ("
  echo "\$s" | sed -e "/^ *$/ d" | sed -e "2,\$ s/^/UNION /"
  echo ")"
  echo "select 
           table_name
          ,source_rows
          ,target_rows
          ,source_rows - target_rows delta 
          ,case 
             when source_rows=target_rows then 'Same Row Count'
             else 'DIFFERENT Row Count'
           end STATUS
  from compare ;"
  )

  #echo "Starting job \$num"
  #echo "$toRun"

sqlplus -s / as sysdba > \$LOG_DIR/result_\$num.txt <<%%
set pages 0
set feed off
set head off
set tab off
set lines 2000
set trimout on
col table_name format a50
col STATUS format a20
col source_rows format "999G999G999G999"
col target_rows format "999G999G999G999"
col delta       format "999G999G999G999"
alter session set container = \$PDB;
\$(echo "\$toRun")
%%
  while [ -f \$LOG_DIR/verif.tmp.lck ]
  do
    sleep 1
  done
  end_date=\$(date "+%d/%m/%Y %H:%M:%S")
  end_date_epoch=\$(date +%s)
  secs=\$((\$end_date_epoch - \$start_date_epoch))
  touch \$LOG_DIR/verif.tmp.lck
  echo "       --+--> Job \$num terminated in \$secs seconds (Start : \$start_date --> End : \$end_date) "
  echo "         |"
  echo "\$toRun" | sed -e "s;^;         |         ;"
  echo "         |"
  echo "         +-----------------------------------------------------------------------"
  echo "         |"
  cat \$LOG_DIR/result_\$num.txt | sed -e "s;^;         |         ;"
  echo "         |"
  echo "         +-----------------------------------------------------------------------"
  rm -f \$LOG_DIR/verif.tmp.lck
  rm -f \$LOG_DIR/result_\$num.txt
%EOF%
}

DB=prd03exa
PDB=bna0prd
MAX_JOBS=15

LOG_DIR=$HOME/verificationSynchro/$DB
mkdir -p $LOG_DIR
rm -f $LOG_DIR/verif.tmp.lck
rm -f $LOG_DIR/result_*.txt

script_start_date=$(date "+%d/%m/%Y %H:%M:%S")
script_start_date_epoch=$(date +%s)


LOG_FILE=$LOG_DIR/runVerif_$(date +%Y%m%d_%H%M%S).log
. $HOME/$DB.env || die "Impossible de positionner l'environnement"
if tty -s
#if false
then
  die "Script à lancer en NOHUP"
fi
genRunStmt
i=0
{
  #
  #     POur tester les experssions régulières, décommenter les lignes ci-dessous,
  # Le MOVE ne fera rien d'autre que d'afficher les commandes , ce qui permet de
  # vérifier que chaque script traite bien les objets souhaites
  #

  echo "============================================================================="
  echo "     Vérification des réplications"
  echo "     Demarrage a : $(date)"
  echo "============================================================================="


  echo " - Analyse des réplications"
  echo "   ================================================"
  echo
  getStmts > /tmp/$$.tmp.sql
  j=0
  i=0
  while read line 
  do
    if [ $i -eq 5 ]
    then
      j=$(($j + 1))
      sh /tmp/runStmt.sh  "$stmt" $j $LOG_DIR $PDB &
      i=0
      stmt=""
    fi
    if [ $i -le 5 ]
    then
      stmt="$stmt
$line"
      i=$(($i + 1))
    fi

    if [ "$(echo "$line" | grep "PARALLEL(64)")" != "" ]
    then
     i=5
    fi

    while [ $(jobs -r | wc -l | tr -d " ") -ge $MAX_JOBS ]
    do
      sleep 1
    done
  done < /tmp/$$.tmp.sql

  if [ "$stmt" != "" ]
  then
      j=$(($j + 1))
      sh /tmp/runStmt.sh  "$stmt" $j $LOG_DIR $PDB &
      i=0
      stmt=""
  fi

  echo "Scripts de deplacement lances ....."
  echo "Attente de la fin des processus"
  echo "==============================="
  echo
#  while [ $(jobs -r | wc -l | tr -d " ") -ge 0 ]
#  do
#    sleep 1
#  done
   wait

script_end_date=$(date "+%d/%m/%Y %H:%M:%S")
script_end_date_epoch=$(date +%s)
script_secs=$(($script_end_date_epoch - $script_start_date_epoch))

echo "+===========================================================================+"
echo "   Script terminated "
echo "   Start date : $script_start_date"
echo "   End date   : $script_end_date"
echo "   Duration   : $script_secs Seconds"
echo "+===========================================================================+"


}  >$LOG_FILE 2>&1
rm -f $LOG_DIR/$$.tmp.lck
rm -f $LOG_DIR/result_*.txt
rm -f /tmp/runStmt.sh
