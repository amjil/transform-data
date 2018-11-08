#!/bin/bash
ip="10.221.175.148"
user="root"
password="123456"

logfile="etlload.log"
if [ ! -f "$logfile" ]; then
  touch "$logfile"
fi

if [ ! -n "$1" ]; then
  echo "code1 param is null!">> ${logfile}
  exit
elif [ ! -n "$2" ]; then
  echo "code2 param is null!">> ${logfile}
  exit
elif [ ! -n "$3" ]; then
  echo "code3 param is null!">> ${logfile}
  exit
elif [ ! -n "$4" ]; then
  echo "code4 param is null!">> ${logfile}
  exit
fi

echo "table name is $4 \n">> ${logfile}
echo "table name is $4 \n">> ${logfile}

sql="load data local infile 'data/$2' into table $4 lines terminated by '\r\n';"
if [ $1 == "1" ]; then
  sql1="alter table $4 truncate partition part_$3;"
  mysql -h${ip} -P 8761 -u${user} -p${password} <<EOFMYSQL
    ${sql1};
    ${sql};
    exit;
  EOFMYSQL
else
  sql1="truncate table $4;"
  mysql -h${ip} -P 8761 -u${user} -p${password} <<EOFMYSQL
    ${sql1};
    ${sql};
    exit;
  EOFMYSQL
fi
