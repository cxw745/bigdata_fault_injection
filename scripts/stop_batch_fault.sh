#!/bin/bash
# =============================================================================
# 批量故障注入终止脚本
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/logs/batch_run.pid"
LOG_DIR="${SCRIPT_DIR}/logs"

echo "============================================================"
echo "批量故障注入终止脚本"
echo "============================================================"

# 查找进程
BATCH_PID=""
PROCESS_FOUND=false

# 首先检查PID文件
if [ -f "${PID_FILE}" ]; then
    BATCH_PID=$(cat "${PID_FILE}")
    if ps -p ${BATCH_PID} > /dev/null 2>&1; then
        PROCESS_FOUND=true
        echo "🟡 通过PID文件找到进程: ${BATCH_PID}"
    else
        echo "ℹ️  PID文件中的进程已结束: ${BATCH_PID}"
    fi
fi

# 如果PID文件无效，搜索进程
if [ "${PROCESS_FOUND}" = false ]; then
    echo "🔍 搜索运行中的进程..."
    
    # 搜索Python进程
    BATCH_PID=$(ps aux 2>/dev/null | grep 'batch_fault_scheduler.py' | grep -v grep | awk '{print $2}' | head -1 || echo "")
    
    if [ -n "${BATCH_PID}" ]; then
        PROCESS_FOUND=true
        echo "🟡 通过进程搜索找到进程: ${BATCH_PID}"
    else
        # 也检查旧的scheduler.py
        BATCH_PID=$(ps aux 2>/dev/null | grep 'scheduler.py' | grep -v grep | awk '{print $2}' | head -1 || echo "")
        if [ -n "${BATCH_PID}" ]; then
            PROCESS_FOUND=true
            echo "🟡 找到旧版scheduler进程: ${BATCH_PID}"
        fi
    fi
fi

if [ "${PROCESS_FOUND}" = true ] && [ -n "${BATCH_PID}" ]; then
    echo ""
    echo "⚠️  警告: 即将终止进程 ${BATCH_PID}"
    echo "   这将停止所有正在进行的故障注入任务"
    echo ""
    
    read -p "确认终止? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🛑 正在终止进程..."
        
        # 发送SIGTERM信号
        kill -TERM ${BATCH_PID} 2>/dev/null || true
        
        # 等待进程结束
        WAIT_COUNT=0
        while ps -p ${BATCH_PID} > /dev/null 2>&1 && [ ${WAIT_COUNT} -lt 10 ]; do
            echo "   等待进程结束... (${WAIT_COUNT}/10)"
            sleep 1
            WAIT_COUNT=$((WAIT_COUNT + 1))
        done
        
        # 如果进程还在，发送SIGKILL
        if ps -p ${BATCH_PID} > /dev/null 2>&1; then
            echo "   进程未响应，发送强制终止..."
            kill -9 ${BATCH_PID} 2>/dev/null || true
            sleep 1
        fi
        
        # 清理PID文件
        rm -f "${PID_FILE}" 2>/dev/null || true
        
        echo "✅ 进程已终止"
    else
        echo "ℹ️  已取消终止"
    fi
else
    echo "🟢 未找到运行中的进程"
fi

# 清理可能的僵尸进程
ZOMBIE_PIDS=$(ps aux 2>/dev/null | grep 'defunct' | grep -v grep | awk '{print $2}' | head -5 || echo "")
if [ -n "${ZOMBIE_PIDS}" ]; then
    echo ""
    echo "🧹 清理僵尸进程..."
    for ZPID in ${ZOMBIE_PIDS}; do
        echo "   清理僵尸进程: ${ZPID}"
        kill -9 ${ZPID} 2>/dev/null || true
    done
    echo "✅ 僵尸进程已清理"
fi

echo ""
echo "============================================================"
echo "常用命令:"
echo "  启动: bash ${SCRIPT_DIR}/start_batch_fault.sh"
echo "  状态: bash ${SCRIPT_DIR}/status_batch_fault.sh"
echo "============================================================"
