if [ "$1" = "" ]
then
  echo "hangAnalyse.sh PDB"
  exit 1 ;
fi
file=$(sqlplus / as sysdba <<%% | grep -i "hang analysis in" | sed "s;^.* in ;;"
alter session set container=$1 ;
oradebug setmypid
oradebug  unlimit
oradebug -g all hanganalyze 3
oradebug -g all dump systemstate 258
%%
)
[ -f $file ] && cat $file || echo "$file not found"
