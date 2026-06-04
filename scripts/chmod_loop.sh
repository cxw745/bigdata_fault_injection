#!/bin/bash
DIR="/hdfs-nfs/tmp/logs/ubuntu/bucket-cxw745-logs-tfile"

while true; do
    sudo -u ubuntu chmod -R a+rx "$DIR"
    # 记录时间可选
    echo "$(date '+%F %T') 执行了一次 chmod -R a+rx $DIR"
    sleep 60   # 等待60秒
done
