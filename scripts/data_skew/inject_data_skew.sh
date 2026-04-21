#!/bin/bash
set -e

BASE_DIR="/scripts/data_skew"
LOCAL_DATA="$BASE_DIR/data/skew_input/skew_3.2gb.txt"
HDFS_IN="/user/hadoop/skew_input"
HDFS_OUT="/user/hadoop/skew_output"

echo "> Step 1: 确认数据存在"
[ ! -f "$LOCAL_DATA" ] && python3 "$BASE_DIR/skew_generator_3.2gb.py"

echo "> Step 2: 上传数据到 HDFS"
hdfs dfs -rm -r -f $HDFS_IN || true
hdfs dfs -mkdir -p $HDFS_IN
hdfs dfs -put -f $LOCAL_DATA $HDFS_IN

echo "> Step 3: 运行 Hadoop Streaming Job (产生数据倾斜)"
hdfs dfs -rm -r -f $HDFS_OUT || true

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $HDFS_IN \
  -output $HDFS_OUT \
  -mapper "$BASE_DIR/skew_mapper.py" \
  -reducer "$BASE_DIR/skew_reducer.py" \
  -file "$BASE_DIR/skew_mapper.py" \
  -file "$BASE_DIR/skew_reducer.py" \
  -numReduceTasks 8 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner


echo "> Step 4: 查看输出和 Reduce 执行情况"
hdfs dfs -ls -h $HDFS_OUT
echo "查看部分结果:"
hdfs dfs -cat $HDFS_OUT/part-* | head -n 10
