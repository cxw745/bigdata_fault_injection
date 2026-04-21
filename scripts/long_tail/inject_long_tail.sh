#!/bin/bash
set -e

JOB_NAME="wordcount_py_$(date +%s)"
BASE_DIR="/scripts/long_tail"
INPUT="/HiBench/HiBench/Wordcount/Input"
OUTPUT="/user/hadoop/long_tail_output"

# 清理旧输出（如有）
hdfs dfs -rm -r -f $OUTPUT || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="$JOB_NAME" \
  -numReduceTasks 8 \
  -file "$BASE_DIR/mapper_long_tail.py" \
  -file "$BASE_DIR/reducer_long_tail.py" \
  -mapper "$BASE_DIR/mapper_long_tail.py" \
  -reducer "$BASE_DIR/reducer_long_tail.py" \
  -input "$INPUT" \
  -output "$OUTPUT"

echo "===== 结果预览 ====="
hdfs dfs -ls -h $OUTPUT
echo "查看部分结果:"
hdfs dfs -cat $OUTPUT/part-* | head -n 10


