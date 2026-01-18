#!/bin/bash
set -e

JOB_NAME="wordcount_py_$(date +%s)"
BASE_DIR="/scripts/common_mapreduce"
INPUT="/HiBench/HiBench/Wordcount/Input"
OUTPUT="/HiBench/HiBench/Wordcount/Output"

# 清理旧输出（如有）
hdfs dfs -rm -r -f $OUTPUT || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="$JOB_NAME" \
  -numReduceTasks 8 \
  -file "$BASE_DIR/mapper.py" \
  -file "$BASE_DIR/reducer.py" \
  -mapper "$BASE_DIR/mapper.py" \
  -reducer "$BASE_DIR/reducer.py" \
  -input "$INPUT" \
  -output "$OUTPUT"

echo "===== 结果预览 ====="
hdfs dfs -ls -h $HDFS_OUT
echo "查看部分结果:"
hdfs dfs -cat $HDFS_OUT/part-* | head -n 10


