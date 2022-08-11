declare -a PIDS
DB=prd01exa
PDB=bna0prd

SCRIPT_DIR=/admindb/technique/reduceTablespaces
LOG_DIR=$HOME/reduceTablespace/$DB
mkdir -p $LOG_DIR

LOG_FILE=$LOG_DIR/runMove_$(date +%Y%m%d_%H%M%S).log
LOCK_FILE=$LOG_FILE.lck
touch $LOCK_FILE
i=0
{
  RUN=15                           # Durée max
  TBS=TBS_BNA0PRD_BNA_ACTIVE       # Tablespace

  GENERATE=""
  SLEEP=60                         # Attente entre chaque lancement

  #
  #     POur tester les experssions régulières, décommenter les lignes ci-dessous,
  # Le MOVE ne fera rien d'autre que d'afficher les commandes , ce qui permet de
  # vérifier que chaque script traite bien les objets souhaites
  #
#  GENERATE="-g"
#  SLEEP=1

  echo "============================================================================="
  echo "     Deplacement des segments de tablespaces"
  echo "     Demarrage a : $(date)"
  echo "============================================================================="

if false
then
  i=$(($i + 1))
  TABLE_IN1="^RES_RESSOURCE_RETENUE$"
  TABLE_IN2="^RES_RESSOURCE_RETENUE_IX.*"
  TABLE_OUT="##NONE##"
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN1 / TABLE_OUT=$TABLE_OUT puis TABLE_IN=$TABLE_IN2 / TABLE_OUT=$TABLE_OUT puis"
  ( $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN1}"                 -P 48 -R $RUN -F -U NEVER $GENERATE >/dev/null 2>&1 ; \
    $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN2}"                 -P 24 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 ) &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  sleep 900  # On laisse bosser 15 minutes seul

  i=$(($i + 1))
  TABLE_IN1="^RES_RESSOURCE$"
  TABLE_IN2="^(RES_RESSOURCE_IX.*|RES_RESSOURCE_UN.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN1 / TABLE_OUT=$TABLE_OUT puis TABLE_IN=$TABLE_IN2 / TABLE_OUT=$TABLE_OUT"
  ( $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN1}"                 -P 32 -R $RUN -F -U NEVER $GENERATE >/dev/null 2>&1 ; \
    $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN2}"                 -P 24 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 ) &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  sleep 900 # On laisse bosser 15 minutes seul avec RES_RESSOURCE_RETENUE

  i=$(($i + 1))
  TABLE_IN="^(RES_RESSOURCE_DETAIL_BRM.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  i=$(($i + 1))
  TABLE_IN="^(GPI_DROIT.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  i=$(($i + 1))
  TABLE_IN="^(PER_RES_BASE_QUOTIENT_FAMILIAL.*|PER_RES_BASE_ANALYSE.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  i=$(($i + 1))
  TABLE_IN="^(DEM_DEMUSAGER_CONTENU.*|^RES_ANALYSE_RESSOURCE.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  #i=$(($i + 1))
  #TABLE_IN="^PER_RES_BASE_ANALYSE.*"
  #TABLE_OUT="##NONE##"
  #sleep $SLEEP 
  #echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  #$SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 8  -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  #PIDS[$i]=$!
  #echo "          >> PID=${PIDS[$i]}"

  i=$(($i + 1))
  TABLE_IN="^RES_MODALITE_CONTRACTUELLE.*"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  i=$(($i + 1))
  TABLE_IN="^(PER_RES_BASE|PER_RES_BASE_IX.*|PER_RES_BASE_UN.*|RES_ENREGISTREMENT.*|RES_CONTEXTE_PROFESSIONNEL.*|RES_EVENEMENT_PROFESSIONNEL.*|PER_SITPRO.*)$"
  TABLE_OUT="##NONE##"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -t "${TABLE_IN}"                 -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"
fi

  i=$(($i + 1))
  TABLE_IN="^.*$"
  TABLE_OUT="^(DEM_DEMUSAGER_CONTENU.*|RES_RESSOURCE|RES_RESSOURCE_IX.*|RES_MODALITE_CONTRACTUELLE.*|RES_RESSOURCE_RETENUE.*|RES_RESSOURCE_DETAIL_BRM.*|PER_RES_BASE_ANALYSE.*|GPI_DROIT.*|PER_RES_BASE_QUOTIENT_FAMILIAL.*|RES_ANALYSE_RESSOURCE.*|RES_RESSOURCE_UN.*|PER_RES_BASE.*|RES_ENREGISTREMENT.*|RES_CONTEXTE_PROFESSIONNEL.*|RES_EVENEMENT_PROFESSIONNEL.*|PER_SITPRO.*)$"
  sleep $SLEEP 
  echo "     - Lancement #$i : TABLE_IN=$TABLE_IN / TABLE_OUT=$TABLE_OUT"
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS                -x "${TABLE_OUT}" -P 16 -R $RUN -F -U END $GENERATE >/dev/null 2>&1 &
  PIDS[$i]=$!
  echo "          >> PID=${PIDS[$i]}"

  echo "Scripts de deplacement lances ....."
  sleep 10
  echo "Fichiers logs : "
  echo "============="
  find $LOG_DIR -newer $LOCK_FILE

  echo "Attente de la fin des processus"
  echo "==============================="
  echo
  for pid in ${PIDS[*]}; do
    wait $pid
    status=$?
    echo "                  Fin process $pid : Status=$status"
  done


  echo "============================================================================="
  echo " Lancement Voituer Balai, a : $(date)"
  echo "============================================================================="
  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -R 5 -P 32 -F -U END $GENERATE #>/dev/null 2>&1
#  sleep $SLEEP 
#  $SCRIPT_DIR/reduceTablespace.sh -d $DB -p $PDB -T $TBS -R 5 -P 16 -F -U END $GENERATE #>/dev/null 2>&1
  echo "============================================================================="
  echo "     Fin       a : $(date)"
  echo "============================================================================="
  mv /admindb/work/reduceTablespace_MOVE_LOG.log $LOG_DIR
} > $LOG_FILE 2>&1
rm -f $LOCK_FILE

grep "ForAnalysis" $LOG_DIR/*.log | cut -f3-40 -d":" >$LOG_DIR/globalResult.csv
egrep "^ *MOVE;|^ *REBUILD;" $LOG_DIR/*.log | sed -e "s/:/;/" | sed -e "s;^$LOG_DIR/;;" >$LOG_DIR/fullResult.csv
