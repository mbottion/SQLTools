usage()
{
  echo "hangAnalyse.sh [PDB]"
  exit 1 ;
}
[ "$1" = "-?" -o "$1" = "-h" ] && usage
if [ "$1" != "" ]
then
  goPDB="alter session set container=$1 ;"
fi
file=$(sqlplus / as sysdba <<%% | grep -i "hang analysis in" | sed "s;^.* in ;;"

$goPDB

oradebug setmypid
oradebug  unlimit
oradebug -g all hanganalyze 3
oradebug -g all dump systemstate 258
%%
)
echo "

=====================================================================================

     Hang analysis done

     Full Trace File is : $file

=====================================================================================
HANGS from the last analysis
=====================================================================================
"
firstHangOfLastaAnalysis=$(grep -n "HANG ANALYSIS:" $file | tail -1 | cut -f1 -d ":")
firstHangOfLastaAnalysis=$(($firstHangOfLastaAnalysis - 2))
echo
echo "Start line : $firstHangOfLastaAnalysis"
echo "==========="
echo

sed -n "$firstHangOfLastaAnalysis,/END OF HANG ANALYSIS/ p" $file
echo "
=====================================================================================
To have all information and dumps, get the
$file file
=====================================================================================
"
