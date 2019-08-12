#!/usr/bin/env bash

export PATH=$PATH

SYSTEM_DATE=`date +%Y-%m-%d-%H -d '-1 hours'`

YEAR=`echo ${SYSTEM_DATE} | awk -F '-' '{print $1}'`
MONTH=`echo ${SYSTEM_DATE} | awk -F '-' '{print $2}'`
DAY=`echo ${SYSTEM_DATE} | awk -F '-' '{print $1$2$3}'`
HOUR=`echo ${SYSTEM_DATE} | awk -F '-' '{print $4}'`

# echo ${YEAR} ${MONTH} ${DAY} ${HOUR}

# hive -e "load data inpath '/data/user/${DAY}/${HOUR}' into table test.t_user partition(ym='${ym}',day='${day}',hm='${h}');";

hive -e "alter table test.t_user add partition(day='${DAY}', hour='${HOUR}') location '/data/user/${DAY}/${HOUR}'";