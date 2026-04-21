#!/bin/bash
# =============================================================================
# 批量故障注入启动脚本
# 使用nohup和screen确保SSH中断后继续运行
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER_SCRIPT="${SCRIPT_DIR}/collect_data/batch_fault_scheduler.py"
CONFIG_FILE="${SCRIPT_DIR}/collect_data/fault_config_10tests.json"
OUTPUT_BASE="/tmp/fault_test_results"
LOG_DIR="${SCRIPT_DIR}/logs"

# 创建日志目录
mkdir -p "${LOG_DIR}"
mkdir -p "${OUTPUT_BASE}"

# 生成唯一运行ID
RUN_ID=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/batch_run_${RUN_ID}.log"
PID_FILE="${LOG_DIR}/batch_run.pid"

echo "============================================================"
echo "批量故障注入启动脚本"
echo "============================================================"
echo "运行ID: ${RUN_ID}"
echo "调度脚本: ${SCHEDULER_SCRIPT}"
echo "配置文件: ${CONFIG_FILE}"
echo "输出目录: ${OUTPUT_BASE}"
echo "日志文件: ${LOG_FILE}"
echo "============================================================"

# 检查Python脚本是否存在
if [ ! -f "${SCHEDULER_SCRIPT}" ]; then
    echo "❌ 错误: 找不到调度脚本 ${SCHEDULER_SCRIPT}"
    exit 1
fi

# 检查配置文件是否存在
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "⚠️  警告: 找不到配置文件 ${CONFIG_FILE}"
    echo "   将使用默认配置"
    CONFIG_FLAG=""
else
    CONFIG_FLAG="--config ${CONFIG_FILE}"
fi

# 检查是否已有运行的进程
if [ -f "${PID_FILE}" ]; then
    OLD_PID=$(cat "${PID_FILE}")
    if ps -p "${OLD_PID}" > /dev/null 2>&1; then
        echo "⚠️  检测到已有进程在运行 (PID: ${OLD_PID})"
        echo "   请先停止旧进程或等待其完成"
        echo ""
        echo "查看状态: bash ${SCRIPT_DIR}/status_batch_fault.sh"
        echo "停止进程: bash ${SCRIPT_DIR}/stop_batch_fault.sh"
        exit 1
    else
        echo "ℹ️  清理旧的PID文件"
        rm -f "${PID_FILE}" 2>/dev/null || true
    fi
fi

# 使用nohup启动，确保SSH中断后继续运行
echo "🚀 启动批量故障注入..."
echo "   使用nohup确保SSH中断后继续运行"
echo ""

nohup python3 "${SCHEDULER_SCRIPT}" \
    --output-dir "${OUTPUT_BASE}" \
    ${CONFIG_FLAG} \
    > "${LOG_FILE}" 2>&1 &

# 获取进程PID
BATCH_PID=$!
echo "${BATCH_PID}" > "${PID_FILE}"

echo "✅ 已启动!"
echo ""
echo "进程信息:"
echo "  PID: ${BATCH_PID}"
echo "  日志: ${LOG_FILE}"
echo "  输出: ${OUTPUT_BASE}"
echo ""
echo "常用命令:"
echo "  查看日志: tail -f ${LOG_FILE}"
echo "  查看状态: bash ${SCRIPT_DIR}/status_batch_fault.sh"
echo "  停止运行: bash ${SCRIPT_DIR}/stop_batch_fault.sh"
echo ""
echo "============================================================"

# 等待2秒后检查进程是否正常运行
sleep 2
if ps -p ${BATCH_PID} > /dev/null 2>&1; then
    echo "✅ 进程正在运行"
    exit 0
else
    echo "❌ 错误: 进程启动失败"
    echo "请查看日志: ${LOG_FILE}"
    exit 1
fi
