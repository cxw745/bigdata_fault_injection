#!/bin/bash
# Hadoop集群故障注入调度器 - 后台管理脚本
# 用法: ./batch_scheduler.sh start|status|stop

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="/project/data/data_scripts/collect_data/data"
LOG_DIR="/project/data/data_scripts/logs"
PID_FILE="/tmp/batch_scheduler.pid"
BATCH_FILE="/tmp/batch_scheduler.batch"

# 调度参数（可通过环境变量覆盖）
TOTAL_RUNS=${TOTAL_RUNS:-300}
NORMAL_RATIO=${NORMAL_RATIO:-0.3}
FAULT_TYPES=${FAULT_TYPES:-"wait_time,exit_time"}
INTERVAL_MIN=${INTERVAL_MIN:-60}
INTERVAL_MAX=${INTERVAL_MAX:-120}
DATA_SIZE=${DATA_SIZE:-"small"}
MAX_BATCH_SIZE=${MAX_BATCH_SIZE:-50}

# 内存监控参数
MEMORY_THRESHOLD=${MEMORY_THRESHOLD:-500}  # 内存阈值(MB)，低于此值时清理内存
MEMORY_CLEANUP_INTERVAL=${MEMORY_CLEANUP_INTERVAL:-10}  # 每N个任务检查一次内存

# Python调度器
SCHEDULER="${SCRIPT_DIR}/collect_data/unified_scheduler_v2.py"

mkdir -p "${DATA_DIR}" "${LOG_DIR}"

# 获取可用内存(MB)
get_available_memory() {
    local mem_available=$(cat /proc/meminfo | grep MemAvailable | awk '{print $2}')
    local mem_available_mb=$((mem_available / 1024))
    echo ${mem_available_mb}
}

# 获取内存使用率
get_memory_usage() {
    local mem_info=$(free | grep Mem)
    local total=$(echo ${mem_info} | awk '{print $2}')
    local used=$(echo ${mem_info} | awk '{print $3}')
    local usage=$((used * 100 / total))
    echo ${usage}
}

# 清理内存
cleanup_memory() {
    local force=${1:-false}
    local mem_available=$(get_available_memory)
    local mem_usage=$(get_memory_usage)
    
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                           内存清理                                           │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    printf "│ %-20s: %-52s │\n" "清理前可用内存" "${mem_available} MB"
    printf "│ %-20s: %-52s │\n" "内存使用率" "${mem_usage}%"
    
    # 执行清理
    sync
    # 尝试使用sudo清理，如果失败则使用sync
    if ! (echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1); then
        echo "  注意: 无法清理系统缓存(需要sudo权限)，已执行sync"
    fi
    
    # 等待内存回收
    sleep 2
    
    local mem_available_after=$(get_available_memory)
    local mem_freed=$((mem_available_after - mem_available))
    
    printf "│ %-20s: %-52s │\n" "清理后可用内存" "${mem_available_after} MB"
    printf "│ %-20s: %-52s │\n" "释放内存" "+${mem_freed} MB"
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    
    # 如果内存仍然不足，给出警告
    if [ ${mem_available_after} -lt ${MEMORY_THRESHOLD} ]; then
        echo "⚠️ 警告: 内存仍然不足 (${mem_available_after}MB < ${MEMORY_THRESHOLD}MB)"
        echo "建议: 减少并发任务数或增加物理内存"
        return 1
    fi
    
    return 0
}

# 检查内存并清理（如果必要）
check_and_cleanup_memory() {
    local task_count=${1:-0}
    local mem_available=$(get_available_memory)
    
    # 每N个任务检查一次内存，或内存低于阈值时
    if [ $((task_count % MEMORY_CLEANUP_INTERVAL)) -eq 0 ] || [ ${mem_available} -lt ${MEMORY_THRESHOLD} ]; then
        echo ""
        echo "内存检查: 可用 ${mem_available}MB (阈值: ${MEMORY_THRESHOLD}MB)"
        
        if [ ${mem_available} -lt ${MEMORY_THRESHOLD} ]; then
            echo "⚠️ 内存低于阈值，执行清理..."
            cleanup_memory
            return $?
        fi
    fi
    
    return 0
}

get_current_batch_dir() {
    if [ -f "${BATCH_FILE}" ]; then
        cat "${BATCH_FILE}"
    else
        echo ""
    fi
}

get_batch_progress() {
    local batch_dir="$1"
    if [ -z "${batch_dir}" ] || [ ! -d "${batch_dir}" ]; then
        echo "0 0"
        return
    fi
    
    local csv_file="${batch_dir}/execution_records.csv"
    if [ ! -f "${csv_file}" ]; then
        echo "0 0"
        return
    fi
    
    # 使用awk正确解析CSV，注意CSV字段带有引号
    local completed=$(awk -F',' 'NR>1 && NF>=6 {count++} END {print count+0}' "${csv_file}" 2>/dev/null)
    local success=$(awk -F',' 'NR>1 && NF>=6 && $6=="\"True\"" {count++} END {print count+0}' "${csv_file}" 2>/dev/null)
    
    completed=${completed:-0}
    success=${success:-0}
    echo "${completed} ${success}"
}

get_all_batches_summary() {
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                           历史批次执行统计                                    │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    
    local total_runs=0
    local total_success=0
    local batch_count=0
    local all_fault_types=""
    
    for batch_dir in $(ls -d ${DATA_DIR}/batch_* 2>/dev/null | sort); do
        local batch_name=$(basename "${batch_dir}")
        local csv_file="${batch_dir}/execution_records.csv"
        
        if [ -f "${csv_file}" ]; then
            # 使用awk正确解析CSV，处理包含逗号的字段
            # 注意: CSV中的字段带有引号，需要比较 "True" 而不是 True
            local runs=$(awk -F',' 'NR>1 && NF>=6 {count++} END {print count+0}' "${csv_file}" 2>/dev/null)
            local success_count=$(awk -F',' 'NR>1 && NF>=6 && $6=="\"True\"" {count++} END {print count+0}' "${csv_file}" 2>/dev/null)
            
            # 获取故障类型分布（去引号后统计）
            local fault_dist=$(awk -F',' 'NR>1 && NF>=2 {gsub(/"/,"",$2); print $2}' "${csv_file}" 2>/dev/null | sort | uniq -c | sort -rn | while read count fault; do
                if [ -n "$fault" ]; then
                    echo "${fault}:${count}"
                fi
            done | tr '\n' ', ' | sed 's/, $//')
            
            # 累加所有故障类型（每行一个，用换行分隔）
            local batch_faults=$(awk -F',' 'NR>1 && NF>=2 {gsub(/"/,"",$2); print $2}' "${csv_file}" 2>/dev/null)
            all_fault_types="${all_fault_types}
${batch_faults}"
            
            runs=${runs:-0}
            success_count=${success_count:-0}
            
            total_runs=$((total_runs + runs))
            total_success=$((total_success + success_count))
            batch_count=$((batch_count + 1))
            
            # 显示批次信息（故障分布换行显示）
            printf "│ %-30s │ %3d次 │ 成功:%3d │\n" "${batch_name}" "${runs}" "${success_count}"
            if [ -n "${fault_dist}" ]; then
                printf "│   故障分布: %-63s │\n" "${fault_dist:0:60}"
            fi
        fi
    done
    
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    printf "│ 批次统计: %d 个批次, %d 次执行, %d 次成功                                  │\n" "${batch_count}" "${total_runs}" "${total_success}"
    
    # 显示所有批次的故障类型汇总
    if [ -n "${all_fault_types}" ]; then
        echo "├─────────────────────────────────────────────────────────────────────────────┤"
        echo "│ 所有批次故障类型汇总:                                                        │"
        echo "${all_fault_types}" | tr ' ' '\n' | grep -v '^$' | sort | uniq -c | sort -rn | while read count fault; do
            if [ -n "$fault" ]; then
                printf "│   %-20s %3d 次                                              │\n" "${fault}" "${count}"
            fi
        done
    fi
    
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
}

start_scheduler() {
    echo "=========================================="
    echo "启动批量调度器"
    echo "=========================================="
    
    # 检查是否已经在运行
    if [ -f "${PID_FILE}" ]; then
        local old_pid=$(cat ${PID_FILE})
        if ps -p ${old_pid} > /dev/null 2>&1; then
            echo "错误: 调度器已在运行 (PID: ${old_pid})"
            echo "请先运行: $0 stop"
            exit 1
        else
            rm -f ${PID_FILE}
        fi
    fi
    
    # 启动前检查内存
    echo ""
    echo "启动前内存检查..."
    local mem_available=$(get_available_memory)
    echo "当前可用内存: ${mem_available}MB (阈值: ${MEMORY_THRESHOLD}MB)"
    
    if [ ${mem_available} -lt ${MEMORY_THRESHOLD} ]; then
        echo "⚠️ 内存不足，执行清理..."
        cleanup_memory || {
            echo "❌ 内存清理后仍然不足，建议先释放内存再启动"
            exit 1
        }
    fi
    
    # 计算任务分布
    local NORMAL_COUNT=$(echo "${TOTAL_RUNS} * ${NORMAL_RATIO}" | bc | cut -d'.' -f1)
    local FAULT_COUNT=$((TOTAL_RUNS - NORMAL_COUNT))
    local FAULT_TYPE_ARRAY=(${FAULT_TYPES//,/ })
    local FAULT_TYPE_COUNT=${#FAULT_TYPE_ARRAY[@]}
    local FAULT_PER_TYPE=$((FAULT_COUNT / FAULT_TYPE_COUNT))
    local REMAINDER=$((FAULT_COUNT % FAULT_TYPE_COUNT))
    
    echo "参数配置:"
    echo "  总任务数: ${TOTAL_RUNS}"
    echo "  每批次上限: ${MAX_BATCH_SIZE}"
    echo "  正常任务比例: ${NORMAL_RATIO} (${NORMAL_COUNT}次)"
    echo "  故障类型: ${FAULT_TYPES}"
    echo "  间隔时间: ${INTERVAL_MIN}-${INTERVAL_MAX}秒"
    echo "  数据大小: ${DATA_SIZE}"
    echo ""
    echo "任务分布:"
    echo "  正常任务(wordcount): ${NORMAL_COUNT}次"
    
    local type_idx=0
    for fault_type in "${FAULT_TYPE_ARRAY[@]}"; do
        local count=${FAULT_PER_TYPE}
        if [ ${type_idx} -lt ${REMAINDER} ]; then
            count=$((count + 1))
        fi
        echo "  ${fault_type}: ${count}次"
        type_idx=$((type_idx + 1))
    done
    
    # 计算需要的批次数
    local BATCH_COUNT=$(( (TOTAL_RUNS + MAX_BATCH_SIZE - 1) / MAX_BATCH_SIZE ))
    echo ""
    echo "批次规划: ${BATCH_COUNT} 个批次"
    echo "=========================================="
    
    # 生成序列字符串
    local SEQUENCE=""
    for i in $(seq 1 ${NORMAL_COUNT}); do
        SEQUENCE="${SEQUENCE}wordcount:1,"
    done
    
    type_idx=0
    for fault_type in "${FAULT_TYPE_ARRAY[@]}"; do
        local count=${FAULT_PER_TYPE}
        if [ ${type_idx} -lt ${REMAINDER} ]; then
            count=$((count + 1))
        fi
        for i in $(seq 1 ${count}); do
            SEQUENCE="${SEQUENCE}${fault_type}:1,"
        done
        type_idx=$((type_idx + 1))
    done
    
    # 去掉末尾逗号
    SEQUENCE="${SEQUENCE%,}"
    
    # 随机打乱顺序
    SEQUENCE=$(echo "${SEQUENCE}" | tr ',' '\n' | shuf | tr '\n' ',' | sed 's/,$//')
    
    echo "生成的序列 (前30个任务):"
    echo "${SEQUENCE}" | tr ',' '\n' | head -30 | nl
    echo "..."
    echo ""
    
    # 构建命令
    local CMD="cd ${SCRIPT_DIR} && python3 ${SCHEDULER} \
        --mode sequential \
        --sequence '${SEQUENCE}' \
        --data-size ${DATA_SIZE} \
        --output-dir '${DATA_DIR}' \
        --interval-min ${INTERVAL_MIN} \
        --interval-max ${INTERVAL_MAX} \
        --max-batch-size ${MAX_BATCH_SIZE}"
    
    echo "启动命令:"
    echo "${CMD}"
    echo ""
    
    # 后台启动
    local LOG_FILE="${LOG_DIR}/batch_scheduler_$(date +%Y%m%d_%H%M%S).log"
    nohup bash -c "${CMD}" > "${LOG_FILE}" 2>&1 &
    local pid=$!
    
    echo ${pid} > ${PID_FILE}
    echo "调度器已启动，PID: ${pid}"
    echo "日志文件: ${LOG_FILE}"
    echo ""
    echo "使用以下命令:"
    echo "  查看状态: $0 status"
    echo "  查看日志: tail -f ${LOG_FILE}"
    echo "  停止调度: $0 stop"
}

check_status() {
    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                         批量调度器运行状态                                     ║"
    echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
    
    # 显示内存状态
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                              内存状态                                        │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    local mem_available=$(get_available_memory)
    local mem_usage=$(get_memory_usage)
    local mem_status="✓ 正常"
    if [ ${mem_available} -lt ${MEMORY_THRESHOLD} ]; then
        mem_status="⚠️ 不足"
    fi
    printf "│ %-20s: %-52s │\n" "可用内存" "${mem_available} MB ${mem_status}"
    printf "│ %-20s: %-52s │\n" "内存使用率" "${mem_usage}%"
    printf "│ %-20s: %-52s │\n" "内存阈值" "${MEMORY_THRESHOLD} MB"
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    
    # 检查进程状态
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                              进程状态                                        │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    
    if [ -f "${PID_FILE}" ]; then
        local pid=$(cat ${PID_FILE})
        if ps -p ${pid} > /dev/null 2>&1; then
            local proc_info=$(ps -p ${pid} -o etime= 2>/dev/null | xargs)
            local cpu_usage=$(ps -p ${pid} -o %cpu= 2>/dev/null | xargs)
            local mem_usage=$(ps -p ${pid} -o %mem= 2>/dev/null | xargs)
            
            printf "│ %-20s: %-52s │\n" "状态" "✓ 运行中"
            printf "│ %-20s: %-52s │\n" "PID" "${pid}"
            printf "│ %-20s: %-52s │\n" "运行时间" "${proc_info}"
            printf "│ %-20s: %-52s │\n" "CPU使用率" "${cpu_usage}%"
            printf "│ %-20s: %-52s │\n" "内存使用率" "${mem_usage}%"
        else
            printf "│ %-20s: %-52s │\n" "状态" "✗ 已停止 (PID文件存在但进程已结束)"
            rm -f ${PID_FILE}
        fi
    else
        printf "│ %-20s: %-52s │\n" "状态" "○ 未运行"
    fi
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    
    # 当前批次进度
    local current_batch=$(get_current_batch_dir)
    
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                           当前批次执行进度                                    │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    
    if [ -n "${current_batch}" ] && [ -d "${current_batch}" ]; then
        local batch_name=$(basename "${current_batch}")
        read completed success <<< $(get_batch_progress "${current_batch}")
        
        printf "│ %-20s: %-52s │\n" "当前批次" "${batch_name}"
        printf "│ %-20s: %-52s │\n" "已完成任务" "${completed} 次"
        printf "│ %-20s: %-52s │\n" "成功任务" "${success} 次"
        
        # 故障类型分布
        local csv_file="${current_batch}/execution_records.csv"
        if [ -f "${csv_file}" ]; then
            echo "├─────────────────────────────────────────────────────────────────────────────┤"
            echo "│ 故障类型分布:                                                                │"
            awk -F',' 'NR>1 && NF>=2 {gsub(/"/,"",$2); print $2}' "${csv_file}" 2>/dev/null | sort | uniq -c | sort -rn | while read count fault; do
                if [ -n "${fault}" ]; then
                    printf "│   %-20s %3d 次                                              │\n" "${fault}" "${count}"
                fi
            done
        fi
        
        # 最近执行记录
        if [ ${completed} -gt 0 ]; then
            echo "├─────────────────────────────────────────────────────────────────────────────┤"
            echo "│ 最近执行记录:                                                                │"
            tail -5 "${csv_file}" 2>/dev/null | while IFS=',' read idx fault start end dur success rest; do
                # 去除字段中的引号
                fault=$(echo "${fault}" | tr -d '"')
                success=$(echo "${success}" | tr -d '"')
                start=$(echo "${start}" | tr -d '"')
                end=$(echo "${end}" | tr -d '"')
                dur=$(echo "${dur}" | tr -d '"')
                
                local status_icon="✓"
                if [[ "${success}" != "True" ]]; then
                    status_icon="✗"
                fi
                printf "│   %s %-12s | %s - %s (%ss)%*s │\n" "${status_icon}" "${fault}" "${start:11:8}" "${end:11:8}" "${dur}" $((20 - ${#dur})) ""
            done
        fi
    else
        printf "│ %-20s: %-52s │\n" "当前批次" "无"
    fi
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    
    # 历史批次统计
    get_all_batches_summary
    
    # 磁盘使用情况
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                              磁盘使用情况                                    │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    local data_size=$(du -sh ${DATA_DIR} 2>/dev/null | cut -f1)
    local log_size=$(du -sh ${LOG_DIR} 2>/dev/null | cut -f1)
    local disk_info=$(df -h /project 2>/dev/null | tail -1)
    local disk_avail=$(echo ${disk_info} | awk '{print $4}')
    local disk_used=$(echo ${disk_info} | awk '{print $5}')
    
    printf "│ %-20s: %-52s │\n" "数据目录大小" "${data_size}"
    printf "│ %-20s: %-52s │\n" "日志目录大小" "${log_size}"
    printf "│ %-20s: %-52s │\n" "磁盘可用空间" "${disk_avail} (${disk_used} 已用)"
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    
    # 最近日志
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│                           最近日志 (最后15行)                                 │"
    echo "├─────────────────────────────────────────────────────────────────────────────┤"
    local latest_log=$(ls -t ${LOG_DIR}/batch_scheduler_*.log 2>/dev/null | head -1)
    if [ -n "${latest_log}" ]; then
        printf "│ 日志文件: %-64s │\n" "$(basename ${latest_log})"
        echo "├─────────────────────────────────────────────────────────────────────────────┤"
        tail -15 "${latest_log}" 2>/dev/null | while read line; do
            printf "│ %s%*s │\n" "${line:0:75}" $((75 - ${#line})) ""
        done
    else
        printf "│ %-75s │\n" "暂无日志文件"
    fi
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
}

stop_scheduler() {
    echo "=========================================="
    echo "停止批量调度器"
    echo "=========================================="
    
    local stopped=0
    
    if [ -f "${PID_FILE}" ]; then
        local pid=$(cat ${PID_FILE})
        if ps -p ${pid} > /dev/null 2>&1; then
            echo "正在停止进程 ${pid}..."
            kill ${pid} 2>/dev/null || true
            sleep 2
            
            # 如果还在运行，强制终止
            if ps -p ${pid} > /dev/null 2>&1; then
                echo "进程未响应，强制终止..."
                kill -9 ${pid} 2>/dev/null || true
                sleep 1
            fi
            
            if ! ps -p ${pid} > /dev/null 2>&1; then
                echo "进程已停止 ✓"
                stopped=1
            fi
        else
            echo "进程未运行"
        fi
        rm -f ${PID_FILE}
    else
        echo "未找到PID文件"
    fi
    
    # 终止所有相关的Python调度器进程
    local python_pids=$(pgrep -f "unified_scheduler_v2.py" 2>/dev/null || true)
    if [ -n "${python_pids}" ]; then
        echo "终止相关Python进程..."
        echo "${python_pids}" | xargs kill 2>/dev/null || true
        stopped=1
    fi
    
    if [ ${stopped} -eq 1 ]; then
        echo ""
        echo "停止完成"
    else
        echo "没有运行中的调度器"
    fi
}

# 显示帮助
show_help() {
    echo "Hadoop集群故障注入调度器"
    echo ""
    echo "用法: $0 {start|status|stop|cleanup}"
    echo ""
    echo "命令:"
    echo "  start    启动后台调度器"
    echo "  status   查看运行状态"
    echo "  stop     停止调度器"
    echo "  cleanup  手动清理内存"
    echo ""
    echo "环境变量配置:"
    echo "  TOTAL_RUNS              总任务数 (默认: 300)"
    echo "  NORMAL_RATIO            正常任务比例 (默认: 0.3)"
    echo "  FAULT_TYPES             故障类型，逗号分隔 (默认: wait_time,exit_time)"
    echo "  INTERVAL_MIN            最小间隔秒数 (默认: 60)"
    echo "  INTERVAL_MAX            最大间隔秒数 (默认: 120)"
    echo "  DATA_SIZE               数据大小 (默认: small)"
    echo "  MAX_BATCH_SIZE          每批次最大任务数 (默认: 50)"
    echo "  MEMORY_THRESHOLD        内存阈值(MB)，低于此值时清理 (默认: 500)"
    echo "  MEMORY_CLEANUP_INTERVAL 每N个任务检查一次内存 (默认: 10)"
    echo ""
    echo "示例:"
    echo "  # 使用默认配置启动"
    echo "  $0 start"
    echo ""
    echo "  # 自定义配置启动"
    echo "  TOTAL_RUNS=100 NORMAL_RATIO=0.2 FAULT_TYPES='wait_time,exit_time' $0 start"
    echo ""
    echo "  # 低内存环境启动（降低内存阈值）"
    echo "  MEMORY_THRESHOLD=300 MEMORY_CLEANUP_INTERVAL=5 $0 start"
    echo ""
    echo "  # 查看状态"
    echo "  $0 status"
    echo ""
    echo "  # 手动清理内存"
    echo "  $0 cleanup"
    echo ""
    echo "  # 停止"
    echo "  $0 stop"
}

case "$1" in
    start)
        start_scheduler
        ;;
    status)
        check_status
        ;;
    stop)
        stop_scheduler
        ;;
    cleanup)
        cleanup_memory true
        ;;
    -h|--help|help)
        show_help
        ;;
    *)
        echo "用法: $0 {start|status|stop|cleanup}"
        echo "运行 '$0 --help' 查看更多信息"
        exit 1
        ;;
esac
