#!/bin/bash

LOCAL_DATA_DIR="/project/data/data_scripts/collect_data/data"
REMOTE_HOST="A40-121"
REMOTE_DATA_DIR="/data/dds-data/cxw/collect_data_v2/data"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}=== 增量同步新数据(v2)到GPU服务器 A40-121 ===${NC}"
echo -e "${BLUE}本地目录:${NC} $LOCAL_DATA_DIR"
echo -e "${BLUE}远程目录:${NC} $REMOTE_HOST:$REMOTE_DATA_DIR"
echo ""

if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo -e "${RED}错误: 本地目录不存在: $LOCAL_DATA_DIR${NC}"
    exit 1
fi

echo -e "${YELLOW}检查远程目录...${NC}"
ssh $REMOTE_HOST "mkdir -p '$REMOTE_DATA_DIR'"

echo -e "${YELLOW}统计本地文件...${NC}"
LOCAL_FILE_COUNT=$(find "$LOCAL_DATA_DIR" -type f | wc -l)
echo "本地文件总数: $LOCAL_FILE_COUNT"

echo -e "${YELLOW}统计远程文件...${NC}"
REMOTE_FILE_COUNT=$(ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -type f 2>/dev/null | wc -l" || echo "0")
echo "远程文件总数: $REMOTE_FILE_COUNT"
echo ""

echo -e "${YELLOW}开始rsync增量同步...${NC}"
echo "参数: -avz --progress (归档模式, 详细输出, 压缩传输)"
echo ""

rsync -avz --progress \
    --exclude='*.tmp' \
    --exclude='*.log' \
    "$LOCAL_DATA_DIR/" \
    "$REMOTE_HOST:$REMOTE_DATA_DIR/"

SYNC_STATUS=$?

echo ""
if [ $SYNC_STATUS -eq 0 ]; then
    echo -e "${GREEN}=== 同步成功完成 ===${NC}"
else
    echo -e "${RED}=== 同步过程中出现错误 (退出码: $SYNC_STATUS) ===${NC}"
fi

echo ""
echo -e "${YELLOW}同步后远程文件统计:${NC}"
NEW_REMOTE_COUNT=$(ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -type f 2>/dev/null | wc -l" || echo "0")
echo "远程文件总数: $NEW_REMOTE_COUNT"
echo "新增文件数: $((NEW_REMOTE_COUNT - REMOTE_FILE_COUNT))"

echo ""
echo -e "${YELLOW}远程目录结构:${NC}"
ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -maxdepth 2 -type d | sort | head -20"

echo ""
echo -e "${GREEN}同步完成！下次运行将只复制新增或修改的文件。${NC}"
