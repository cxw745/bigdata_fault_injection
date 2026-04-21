#!/bin/bash
set -e

JOB_NAME="wordcount_py_$(date +%s)"
BASE_DIR="/scripts/task_fail"
INPUT="/HiBench/HiBench/Wordcount/Input"
OUTPUT="/user/hadoop/task_fail_output"

# 清理旧输出（如有）
hdfs dfs -rm -r -f $OUTPUT || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="$JOB_NAME" \
  -numReduceTasks 8 \
  -file "$BASE_DIR/mapper_task_fail.py" \
  -file "$BASE_DIR/reducer_task_fail.py" \
  -mapper "$BASE_DIR/mapper_task_fail.py" \
  -reducer "$BASE_DIR/reducer_task_fail.py" \
  -input "$INPUT" \
  -output "$OUTPUT"

echo "===== 结果预览 ====="
hdfs dfs -ls -h $OUTPUT
echo "查看部分结果:"
hdfs dfs -cat $OUTPUT/part-* | head -n 10


