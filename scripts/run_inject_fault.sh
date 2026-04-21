#!/bin/bash
cd /scripts || exit
nohup python3 scheduler.py > scheduler.log 2>&1 &
echo "✅ 已后台运行，日志：/scripts/scheduler.log"