#!/usr/bin/env bash

jarDir="/opt/hadoop-examples.jar"
if [ -e $jarDir ]; then
    echo "jar not exsit"
fi

date=`date -d yesterday '+%Y-%m-%d'`

# mr 作业
sudo -u hdfs /usr/bin/yarn jar $jarDir LoggerTransfrom "/hdfs/flume/logs/dt=$date" "/user/hive/warehouse/data/userlogs/dt=$date"

# hive 加载数据作业
sudo -u hdfs /usr/bin/hive -e "alter table hive_user_logs add partition (dt='$date') location '/user/hive/warehouse/data/userlogs/dt=$date';"
