opatch lspatches  | grep "^[0-9][0-9]*;" | while read line
do
  pNum=$(echo $line | cut -f1 -d";")
  pLib=$(echo $line | cut -f2 -d";")
  echo "v=\$(opatch lspatches | grep \"^$pNum\")"
  echo "echo -n $pNum ; [ \"\$v\" = \"\" ] && echo \" ==> *** NON Installe ($pLib) ***\" || echo \" ==> OK\""
done
