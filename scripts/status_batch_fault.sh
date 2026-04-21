#!/bin/bash
# =============================================================================
# 批量故障注入状态查看脚本
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/logs/batch_run.pid"
LOG_DIR="${SCRIPT_DIR}/logs"
OUTPUT_BASE="/tmp/fault_test_results"

echo "============================================================"
echo "批量故障注入状态"
echo "============================================================"

# 检查PID文件
if [ -f "${PID_FILE}" ]; then
    BATCH_PID=$(cat "${PID_FILE}")
    
    if ps -p ${BATCH_PID} > /dev/null 2>&1; then
        echo "🟢 进程状态: 运行中"
        echo "   PID: ${BATCH_PID}"
        
        # 获取进程运行时间
        PROC_START=$(ps -o lstart= -p ${BATCH_PID} 2>/dev/null || echo "未知")
        echo "   启动时间: ${PROC_START}"
        
        # 获取CPU和内存使用
        CPU_MEM=$(ps -o %cpu,%mem= -p ${BATCH_PID} 2>/dev/null | tail -1 || echo "N/A N/A")
        echo "   CPU使用: ${CPU_MEM}%"
        echo "   内存使用: ${CPU_MEM}%"
        
        echo ""
        
        # 显示最近的日志
        LOG_FILE=$(ls -t ${LOG_DIR}/batch_run_*.log 2>/dev/null | head -1 || echo "")
        if [ -n "${LOG_FILE}" ] && [ -f "${LOG_FILE}" ]; then
            echo "📝 最近日志 (最后20行):"
            echo "------------------------------------------------------------"
            tail -20 "${LOG_FILE}" | sed 's/^/   /'
            echo "------------------------------------------------------------"
        fi
        
        # 显示已完成的故障任务
        echo ""
        echo "📊 已完成的故障注入任务:"
        if [ -d "${OUTPUT_BASE}" ]; then
            TASK_COUNT=$(find "${OUTPUT_BASE}" -maxdepth 1 -type d -name "*_job_*" | wc -l)
            echo "   任务数: ${TASK_COUNT}"
            
            if [ ${TASK_COUNT} -gt 0 ]; then
                echo ""
                echo "   最近5个任务:"
                find "${OUTPUT_BASE}" -maxdepth 1 -type d -name "*_job_*" -exec basename {} \; \
                    | sort -r | head -5 | sed 's/^/   - /'
            fi
        else
            echo "   暂无任务"
        fi
        
    else
        echo "🟡 进程状态: 已结束 (PID文件残留)"
        echo "   PID: ${BATCH_PID}"
        echo ""
        echo "📝 最后日志:"
        LOG_FILE=$(ls -t ${LOG_DIR}/batch_run_*.log 2>/dev/null | head -1 || echo "")
        if [ -n "${LOG_FILE}" ] && [ -f "${LOG_FILE}" ]; then
            tail -30 "${LOG_FILE}" | sed 's/^/   /'
        fi
        
        echo ""
        echo "💡 提示: PID文件已过期，可以删除"
        echo "   rm ${PID_FILE}"
    fi
else
    echo "🟢 进程状态: 未运行"
    echo ""
    echo "📊 历史任务:"
    if [ -d "${OUTPUT_BASE}" ]; then
        TASK_COUNT=$(find "${OUTPUT_BASE}" -maxdepth 1 -type d -name "*_job_*" | wc -l)
        echo "   历史任务数: ${TASK_COUNT}"
        
        if [ ${TASK_COUNT} -gt 0 ]; then
            echo ""
            echo "   最近5个任务:"
            find "${OUTPUT_BASE}" -maxdepth 1 -type d -name "*_job_*" -exec basename {} \; \
                | sort -r | head -5 | sed 's/^/   - /'
        fi
    fi
    
    echo ""
    echo "🚀 启动新任务:"
    echo "   bash ${SCRIPT_DIR}/start_batch_fault.sh"
fi

echo ""
echo "============================================================"
echo "常用命令:"
echo "  启动: bash ${SCRIPT_DIR}/start_batch_fault.sh"
echo "  停止: bash ${SCRIPT_DIR}/stop_batch_fault.sh"
echo "  查看日志: tail -f ${LOG_DIR}/batch_run_*.log"
echo "============================================================"
