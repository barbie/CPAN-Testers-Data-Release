#!/usr/bin/bash

BASE=/opt/projects/cpantesters

cd $BASE/release
mkdir -p logs
mkdir -p data

date_format="%Y/%m/%d %H:%M:%S"
echo `date +"$date_format"` "START" >>logs/release.log

perl bin/release.pl --config=data/release.ini

echo `date +"$date_format"` "Compressing Release data..." >>logs/release.log

if [ -f $BASE/release/data/release.db ];
then

  cd $BASE/dbx
  rm -f release.*
  cp $BASE/release/data/release.db .  ; gzip  release.db
  cp $BASE/release/data/release.db .  ; bzip2 release.db
  cp $BASE/release/data/release.csv . ; gzip  release.csv
  cp $BASE/release/data/release.csv . ; bzip2 release.csv

  mkdir -p /var/www/cpandevel/release
  mv release.* /var/www/cpandevel/release

fi

echo `date +"$date_format"` "STOP" >>logs/release.log
