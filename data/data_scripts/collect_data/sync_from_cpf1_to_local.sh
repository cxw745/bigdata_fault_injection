#!/bin/bash

# 在本地执行：从 cxw-1 下载数据到本地 Desktop
# 用法: ./sync_from_cpf1_to_local.sh

# 配置
LOCAL_DATA_DIR="$HOME/Desktop/cpf1_data"
REMOTE_HOST="10.10.3.183"  # cxw-1 的 IP
REMOTE_USER="ubuntu"
REMOTE_DATA_DIR="/project/data/data_scripts/collect_data/data"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 开始从 cxw-1 下载数据到本地 Desktop ===${NC}"
echo -e "${BLUE}远程目录:${NC} $REMOTE_USER@$REMOTE_HOST:$REMOTE_DATA_DIR"
echo -e "${BLUE}本地目录:${NC} $LOCAL_DATA_DIR"
echo ""

# 检查本地目录是否存在，不存在则创建
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo -e "${YELLOW}本地目录不存在，正在创建: $LOCAL_DATA_DIR${NC}"
    mkdir -p "$LOCAL_DATA_DIR"
fi

# 统计远程文件数
echo -e "${YELLOW}统计远程文件...${NC}"
REMOTE_FILE_COUNT=$(ssh $REMOTE_USER@$REMOTE_HOST "find '$REMOTE_DATA_DIR' -type f 2>/dev/null | wc -l" || echo "0")
echo "远程文件总数: $REMOTE_FILE_COUNT"

# 统计本地文件数
echo -e "${YELLOW}统计本地文件...${NC}"
LOCAL_FILE_COUNT=$(find "$LOCAL_DATA_DIR" -type f 2>/dev/null | wc -l || echo "0")
echo "本地文件总数: $LOCAL_FILE_COUNT"
echo ""

# 使用rsync进行增量下载
echo -e "${YELLOW}开始rsync增量下载...${NC}"
echo "参数: -avz --progress (归档模式, 详细输出, 压缩传输)"
echo ""

# 执行rsync（源是远程，目标是本地）
rsync -avz --progress \
    --exclude='*.tmp' \
    --exclude='*.log' \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DATA_DIR/" \
    "$LOCAL_DATA_DIR/"

SYNC_STATUS=$?

echo ""
if [ $SYNC_STATUS -eq 0 ]; then
    echo -e "${GREEN}=== 下载成功完成 ===${NC}"
else
    echo -e "${RED}=== 下载过程中出现错误 (退出码: $SYNC_STATUS) ===${NC}"
fi

# 显示下载后的统计
echo ""
echo -e "${YELLOW}下载后本地文件统计:${NC}"
NEW_LOCAL_COUNT=$(find "$LOCAL_DATA_DIR" -type f 2>/dev/null | wc -l || echo "0")
echo "本地文件总数: $NEW_LOCAL_COUNT"
echo "新增文件数: $((NEW_LOCAL_COUNT - LOCAL_FILE_COUNT))"

# 显示本地目录结构
echo ""
echo -e "${YELLOW}本地目录结构:${NC}"
find "$LOCAL_DATA_DIR" -maxdepth 2 -type d | sort | head -20

echo ""
echo -e "${GREEN}下载完成！文件保存在: $LOCAL_DATA_DIR${NC}"
