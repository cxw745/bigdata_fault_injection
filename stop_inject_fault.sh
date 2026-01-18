#!/bin/bash
pid=$(ps aux | grep 'scheduler.py' | grep -v grep | awk '{print $2}')
if [ -n "$pid" ]; then
  kill "$pid"
  echo "🛑 已杀死进程 $pid"
else
  echo "❎ 没找到运行中的 scheduler.py"
fi