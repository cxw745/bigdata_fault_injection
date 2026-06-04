#!/bin/bash
# 快速增量同步脚本
# 用法: ./sync_to_cxw_quick.sh

LOCAL_DIR="/project/data/data_scripts/collect_data/data"
REMOTE="A40-123:/data/dds-data/cxw/collect_data/data"

echo "=== 增量同步到GPU服务器 ==="
echo "本地: $LOCAL_DIR"
echo "远程: $REMOTE"
echo ""

# 使用rsync增量同步
rsync -avz --progress \
    --exclude='*.tmp' \
    --exclude='*.log' \
    "$LOCAL_DIR/" \
    "$REMOTE/"

echo ""
echo "=== 同步完成 ==="
