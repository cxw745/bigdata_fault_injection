#!/bin/bash
set -e

# ===========================
# 一键式 3.2GB 数据膨胀 Fault Injection
# ===========================

BASE_DIR="/scripts/data_bloat"
LOCAL_DATA="$BASE_DIR/data/bloat_input/bloat_3.2gb.txt"
HDFS_IN="/HiBench/HiBench/Wordcount/Input"
HDFS_OUT="/user/hadoop/bloat_output"
GEN_SCRIPT="$BASE_DIR/bloat_generator_3.2gb.py"
MAPPER="$BASE_DIR/mapper_bloat.py"

echo "============================================"
echo "▶ Fault Injection: Data Bloat (3.2GB → 32GB+)"
echo "============================================"

# # Step 1. 生成 3.2GB 原始数据
# echo "▶ Step 1. 生成 3.2GB 原始数据..."
# if [ ! -f "$LOCAL_DATA" ]; then
#     echo "▶ 运行数据生成脚本..."
#     python "$GEN_SCRIPT"
# else
#     echo "✔ 已存在本地数据文件：$LOCAL_DATA"
#     ls -lh "$LOCAL_DATA"
# fi

# # Step 2. 上传原始数据到 HDFS
# echo "▶ Step 2. 上传数据到 HDFS..."
# hdfs dfs -rm -r -f $HDFS_IN || true
# hdfs dfs -mkdir -p $HDFS_IN
# hdfs dfs -put -f $LOCAL_DATA $HDFS_IN

# Step 3. 写入 Mapper（10× 膨胀）
echo "▶ Step 3. 准备 Mapper（每行输出 4 行 = 4× 膨胀）..."
cat << 'EOF' > $MAPPER
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    text = line.strip()
    # 每行复制 4 份
    for i in range(4):
        print(f"{text}\t{i}")
EOF
chmod +x $MAPPER

# Step 4. 运行 Streaming MapReduce（执行数据膨胀）
echo "▶ Step 4. 运行 MapReduce（将产生 10× 中间数据）..."

hdfs dfs -rm -r -f $HDFS_OUT || true

hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $HDFS_IN \
  -output $HDFS_OUT \
  -mapper "$MAPPER" \
  -reducer cat \
  -file "$MAPPER" \
  -numReduceTasks 8

echo "✔ MapReduce Job 已完成"

# Step 5. 自动检测膨胀是否成功
echo "▶ Step 5. 自动检测是否发生数据膨胀..."

JOB_ID=$(yarn application -list -appStates FINISHED | tail -n 1 | awk '{print $1}')

if [[ "$JOB_ID" == "" ]]; then
    echo "✘ 未找到已完成任务，无法检测"
    exit 1
fi

echo "✔ 检测 Job: $JOB_ID"

echo "▶ 读取 Map 输出记录数（map output records）..."
MAP_OUTPUT=$(yarn logs -applicationId "$JOB_ID" 2>/dev/null \
    | grep "Map output records" \
    | awk '{print $NF}' \
    | tail -1)

if [[ "$MAP_OUTPUT" == "" ]]; then
    echo "✘ 无法获取 Map 输出记录数"
    exit 1
fi

# 原始行数（近似）
RAW_LINES=$(wc -l < "$LOCAL_DATA")

echo "原始行数：$RAW_LINES"
echo "Map 输出记录数：$MAP_OUTPUT"

# 理论膨胀目标
EXPECTED=$((RAW_LINES * 10))

echo "理论膨胀目标：$EXPECTED"

if (( MAP_OUTPUT > EXPECTED * 3 / 10 )); then
    echo "✔✔✔ 数据膨胀成功（≈10×）"
else
    echo "✘ 数据膨胀不明显，请检查 Mapper 是否执行"
fi

echo "============================================"
echo "✔ Fault Injection Complete: Data Bloat 3.2GB"
echo "============================================"
