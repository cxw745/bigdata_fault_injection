#!/bin/bash

# 增量同步脚本：将本地数据同步到GPU服务器
# 使用rsync实现高效的增量同步

# 配置
LOCAL_DATA_DIR="/project/data/data_scripts/collect_data/data"
REMOTE_HOST="A40-123"
REMOTE_DATA_DIR="/data/dds-data/cxw/collect_data/data"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 开始增量同步数据到GPU服务器 ===${NC}"
echo -e "${BLUE}本地目录:${NC} $LOCAL_DATA_DIR"
echo -e "${BLUE}远程目录:${NC} $REMOTE_HOST:$REMOTE_DATA_DIR"
echo ""

# 检查本地目录是否存在
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo -e "${RED}错误: 本地目录不存在: $LOCAL_DATA_DIR${NC}"
    exit 1
fi

# 创建远程目录（如果不存在）
echo -e "${YELLOW}检查远程目录...${NC}"
ssh $REMOTE_HOST "mkdir -p '$REMOTE_DATA_DIR'"

# 统计本地文件数
echo -e "${YELLOW}统计本地文件...${NC}"
LOCAL_FILE_COUNT=$(find "$LOCAL_DATA_DIR" -type f | wc -l)
echo "本地文件总数: $LOCAL_FILE_COUNT"

# 统计远程文件数
echo -e "${YELLOW}统计远程文件...${NC}"
REMOTE_FILE_COUNT=$(ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -type f 2>/dev/null | wc -l" || echo "0")
echo "远程文件总数: $REMOTE_FILE_COUNT"
echo ""

# 使用rsync进行增量同步
echo -e "${YELLOW}开始rsync增量同步...${NC}"
echo "参数: -avz --progress (归档模式, 详细输出, 压缩传输)"
echo ""

# 执行rsync
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

# 显示同步后的统计
echo ""
echo -e "${YELLOW}同步后远程文件统计:${NC}"
NEW_REMOTE_COUNT=$(ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -type f 2>/dev/null | wc -l" || echo "0")
echo "远程文件总数: $NEW_REMOTE_COUNT"
echo "新增文件数: $((NEW_REMOTE_COUNT - REMOTE_FILE_COUNT))"

# 显示远程目录结构
echo ""
echo -e "${YELLOW}远程目录结构:${NC}"
ssh $REMOTE_HOST "find '$REMOTE_DATA_DIR' -maxdepth 2 -type d | sort | head -20"

echo ""
echo -e "${GREEN}同步完成！下次运行将只复制新增或修改的文件。${NC}"
