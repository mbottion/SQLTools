die()
{
  echo "
   ERREUR : $*
  "
 exit 1
}
[ "$1" = "" ] && die "Fichier trace non spcifie"
[ ! -f $1 ] && die  "Fichier $1 introuvable"
f=$(basename $1)

if [ "$(file $1 | grep -i ASCII)" = "" ]
then
  cmd="zcat $1"
else
  cmd="cat $1"
fi

echo ""
echo "--+--- Fichier trace: $f ($(hostname))"
echo "  |"
eval $cmd  | \
  sed -e "s;\(ospid *[0-9]*\);(ospid NNNNNN);" | \
  awk '
BEGIN {jour="";test_print=0; error=""; nb=0;}
/^[0-9]{4}-[0-9]{2}-[0-9]{2}/ {
  heure=substr($0,12)
  jour_prec=jour
  jour=substr($0,1,10)
#  printf("                           - %s\n",$0)
  test_print=1
}
/ORA-0$/ {next}
/in tablespace SYSAUX/ {next}
/ORA-/ { error=$0 }
{
  if ( test_print==1)
  {
    if (jour != jour_prec) 
    {
      if ( nb > 1 )
      {
        printf("  |   |                              (repetee %d fois, jusqu a %s)\n",nb,heure)
        nb=0
        prev_error=""
      }
      printf("  |          \n")
      printf("  +---+-- %s\n",jour)
      printf("  |   |\n")
    }
    if (error != "" )
    {
      if ( error==prev_error)
      {
        nb++
      }
      else
      {
        if ( nb > 1 )
        {
          printf("  |   |                              (repetee %d fois, jusqu a %s)\n",nb,heure)
          printf("  |   |      \n")
          nb=0
          prev_error=""
        }
        printf("  |   +--> %s ==> %s\n",heure,error)
        prev_error=error
        nb=1
        error=""
      }
    }
    test_print=0
  }
}

'
