#!/bin/bash
# Hadoop集群故障注入调度器 - 后台管理脚本
# 用法: ./batch_scheduler.sh start|status|stop

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="/project/data/data_scripts/collect_data/data_medium"
LOG_DIR="/project/data/data_scripts/logs"
PID_FILE="/tmp/batch_scheduler.pid"
BATCH_FILE="/tmp/batch_scheduler.batch"

# 调度参数（可通过环境变量覆盖）
TOTAL_RUNS=${TOTAL_RUNS:-5000}
NORMAL_RATIO=${NORMAL_RATIO:-0.90}
FAULT_TYPES=${FAULT_TYPES:-"data_skew,task_fail,long_tail,network_latency,network_loss,data_bloat,wait_time,runtime_delta,exit_time,log_level_change,process_restart,heartbeat_timeout,disk_error,disk_full"}
INTERVAL_MIN=${INTERVAL_MIN:-30}
INTERVAL_MAX=${INTERVAL_MAX:-120}
DATA_SIZE=${DATA_SIZE:-"medium"}
MAX_BATCH_SIZE=${MAX_BATCH_SIZE:-50}
WORKLOAD=${WORKLOAD:-"wordcount"}

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
    # === 续采逻辑：精确补缺口 ===
    local DATA_DIR_RESUME="${DATA_DIR:-/project/data/data_scripts/collect_data/data_medium}"
    if [ -d "$DATA_DIR_RESUME" ]; then
        # 精确统计已采集的各故障类型数量（排除header行）
        local RESUME_INFO=$(python3 -c "
import csv, os
from collections import Counter
base = '$DATA_DIR_RESUME'
c = Counter()
for batch in os.listdir(base):
    bp = os.path.join(base, batch)
    csv_path = os.path.join(bp, 'fault_labels.csv')
    if os.path.exists(csv_path):
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                ft = row.get('fault_type','')
                if ft and ft != 'fault_type':
                    c[ft] += 1
normal_done = c.get('wordcount', 0)
fault_done = {k:v for k,v in c.items() if k != 'wordcount'}
fault_total = sum(fault_done.values())
total_done = normal_done + fault_total
# 输出: total_done normal_done fault_type1:count1 fault_type2:count2 ...
parts = [str(total_done), str(normal_done)]
for ft, cnt in sorted(fault_done.items()):
    parts.append(f'{ft}:{cnt}')
print(' '.join(parts))
" 2>/dev/null)

        if [ -n "$RESUME_INFO" ]; then
            local ALREADY_TOTAL=$(echo $RESUME_INFO | awk '{print $1}')
            local ALREADY_NORMAL=$(echo $RESUME_INFO | awk '{print $2}')

            if [ "$ALREADY_TOTAL" -gt 0 ] 2>/dev/null; then
                local REMAINING=$((TOTAL_RUNS - ALREADY_TOTAL))
                if [ "$REMAINING" -le 0 ]; then
                    echo "已采集 $ALREADY_TOTAL 条数据，达到目标 $TOTAL_RUNS，无需继续"
                    exit 0
                fi
                echo "检测到已采集 $ALREADY_TOTAL 条数据（正常 $ALREADY_NORMAL 条），剩余 $REMAINING 条需要采集"

                # 计算每种故障类型的缺口并生成序列
                local GAP_SEQUENCE=$(python3 -c "
import csv, os
from collections import Counter
base = '$DATA_DIR_RESUME'
c = Counter()
for batch in os.listdir(base):
    bp = os.path.join(base, batch)
    csv_path = os.path.join(bp, 'fault_labels.csv')
    if os.path.exists(csv_path):
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                ft = row.get('fault_type','')
                if ft and ft != 'fault_type':
                    c[ft] += 1

weights = {
    'disk_error': 11, 'heartbeat_timeout': 9, 'network_latency': 9,
    'process_restart': 8, 'task_fail': 7, 'data_skew': 7, 'network_loss': 7,
    'data_bloat': 6, 'wait_time': 6, 'exit_time': 6, 'runtime_delta': 6,
    'long_tail': 6, 'log_level_change': 6, 'disk_full': 6
}
total_weight = sum(weights.values())
TARGET_TOTAL = $TOTAL_RUNS
target_fault = int(TARGET_TOTAL * 0.10)

# 计算缺口
normal_done = c.get('wordcount', 0)
normal_target = TARGET_TOTAL - target_fault
normal_gap = max(0, normal_target - normal_done)

parts = []
if normal_gap > 0:
    parts.append(f'normal:{normal_gap}')

for ft, w in sorted(weights.items(), key=lambda x: -x[1]):
    target_ft = max(30, int(target_fault * w / total_weight))
    done_ft = c.get(ft, 0)
    gap = max(0, target_ft - done_ft)
    if gap > 0:
        parts.append(f'{ft}:{gap}')

print(','.join(parts))
" 2>/dev/null)

                if [ -n "$GAP_SEQUENCE" ]; then
                    echo "补缺口序列: $GAP_SEQUENCE"
                    # 直接使用补缺口序列，跳过原有的序列生成逻辑
                    # 打乱补缺口序列的顺序
                    SHUFFLED_GAP=$(echo "$GAP_SEQUENCE" | tr ',' '
' | shuf | tr '
' ',' | sed 's/,$//')
                    echo "$SHUFFLED_GAP" > /tmp/batch_sequence.txt
                    # 跳转到启动调度器
                    SKIP_SEQUENCE_GEN=1
                else
                    echo "所有故障类型已达标，仅补充正常样本"
                    local NORMAL_GAP=$((TOTAL_RUNS - ALREADY_TOTAL))
                    if [ "$NORMAL_GAP" -gt 0 ]; then
                        echo "normal:$NORMAL_GAP" > /tmp/batch_sequence.txt
                        SKIP_SEQUENCE_GEN=1
                    else
                        echo "已采集 $ALREADY_TOTAL 条数据，达到目标 $TOTAL_RUNS，无需继续"
                        exit 0
                    fi
                fi
            fi
        fi
    fi
    # === 续采逻辑结束 ===
    local SEQ_FILE="/tmp/batch_sequence.txt"
    if [ "$SKIP_SEQUENCE_GEN" = "1" ]; then
        echo "使用补缺口序列，跳过原有序列生成"
    else

    local NORMAL_COUNT=$(echo "${TOTAL_RUNS} * ${NORMAL_RATIO}" | bc | cut -d'.' -f1)
    local FAULT_COUNT=$((TOTAL_RUNS - NORMAL_COUNT))
    local FAULT_TYPE_ARRAY=(${FAULT_TYPES//,/ })
    local FAULT_TYPE_COUNT=${#FAULT_TYPE_ARRAY[@]}
    
    # Weighted fault distribution (based on real-world frequency)
    # Format: fault_type:weight
    local FAULT_WEIGHTS="disk_error:11 heartbeat_timeout:9 network_latency:9 process_restart:8 task_fail:7 data_skew:7 network_loss:7 data_bloat:6 wait_time:6 exit_time:6 runtime_delta:6 long_tail:6 log_level_change:6 disk_full:6"
    
    # Calculate total weight
    local TOTAL_WEIGHT=0
    for fw in ${FAULT_WEIGHTS}; do
        local w=${fw##*:}
        TOTAL_WEIGHT=$((TOTAL_WEIGHT + w))
    done
    
    # Minimum samples per fault type (ensure statistical validity)
    local MIN_PER_TYPE=30
    # 如果故障任务数不足以支持每种故障30条，则按比例缩减
    local MIN_TOTAL_FOR_30=$((FAULT_TYPE_COUNT * 30))
    if [ "$FAULT_COUNT" -lt "$MIN_TOTAL_FOR_30" ]; then
        MIN_PER_TYPE=$((FAULT_COUNT / FAULT_TYPE_COUNT))
        [ "$MIN_PER_TYPE" -lt 1 ] && MIN_PER_TYPE=1
        # 如果故障任务数甚至不够每种1条，则不保证最低分配，完全靠权重
        if [ "$FAULT_COUNT" -lt "$FAULT_TYPE_COUNT" ]; then
            MIN_PER_TYPE=0
        fi
    fi
    
    # Calculate per-fault count: guarantee minimum + distribute remainder by weight
    declare -A FAULT_COUNTS
    local ALLOCATED=0
    
    # Step 1: Allocate minimum to each fault type
    for fw in ${FAULT_WEIGHTS}; do
        local ft=${fw%%:*}
        FAULT_COUNTS[${ft}]=${MIN_PER_TYPE}
        ALLOCATED=$((ALLOCATED + MIN_PER_TYPE))
    done
    
    # Step 2: Distribute remaining slots by weight
    local REMAINDER=$((FAULT_COUNT - ALLOCATED))
    if [ ${REMAINDER} -gt 0 ]; then
        for fw in ${FAULT_WEIGHTS}; do
            local ft=${fw%%:*}
            local w=${fw##*:}
            local extra=$((REMAINDER * w / TOTAL_WEIGHT))
            FAULT_COUNTS[${ft}]=$((FAULT_COUNTS[${ft}] + extra))
            ALLOCATED=$((ALLOCATED + extra))
        done
        
        # Distribute final remainder to highest-weight faults
        local FINAL_REMAINDER=$((FAULT_COUNT - ALLOCATED))
        if [ ${FINAL_REMAINDER} -gt 0 ]; then
            for fw in ${FAULT_WEIGHTS}; do
                [ ${FINAL_REMAINDER} -le 0 ] && break
                local ft=${fw%%:*}
                FAULT_COUNTS[${ft}]=$((FAULT_COUNTS[${ft}] + 1))
                FINAL_REMAINDER=$((FINAL_REMAINDER - 1))
            done
        fi
    fi
    
    echo "参数配置:"
    echo "  总任务数: ${TOTAL_RUNS}"
    echo "  每批次上限: ${MAX_BATCH_SIZE}"
    echo "  正常任务比例: ${NORMAL_RATIO} (${NORMAL_COUNT}次)"
    echo "  故障类型: ${FAULT_TYPES}"
    echo "  间隔时间: ${INTERVAL_MIN}-${INTERVAL_MAX}秒 (Weibull分布 k=1.5 λ=180)"
    echo "  数据大小: ${DATA_SIZE}"
    echo "  Workload: ${WORKLOAD}"
    echo ""
    echo "任务分布 (加权):"
    echo "  正常任务: ${NORMAL_COUNT}次"
    
    for fault_type in "${FAULT_TYPE_ARRAY[@]}"; do
        local count=${FAULT_COUNTS[${fault_type}]:-0}
        echo "  ${fault_type}: ${count}次"
    done
    
    # 计算需要的批次数
    local BATCH_COUNT=$(( (TOTAL_RUNS + MAX_BATCH_SIZE - 1) / MAX_BATCH_SIZE ))
    echo ""
    echo "批次规划: ${BATCH_COUNT} 个批次"
    echo "=========================================="
    
    # 生成序列字符串
    local SEQUENCE=""
    for i in $(seq 1 ${NORMAL_COUNT}); do
        SEQUENCE="${SEQUENCE}normal:1,"
    done
    
    for fault_type in "${FAULT_TYPE_ARRAY[@]}"; do
        local count=${FAULT_COUNTS[${fault_type}]:-0}
        for i in $(seq 1 ${count}); do
            SEQUENCE="${SEQUENCE}${fault_type}:1,"
        done
    done
    
    # 去掉末尾逗号
    SEQUENCE="${SEQUENCE%,}"
    
    # 随机打乱顺序
    SEQUENCE=$(echo "${SEQUENCE}" | tr ',' '\n' | shuf | tr '\n' ',' | sed 's/,$//')
    
    echo "生成的序列 (前30个任务):"
    echo "${SEQUENCE}" | tr ',' '\n' | head -30 | nl
    echo "..."
    echo ""
    
    # 将序列写入临时文件，避免命令行过长
    echo "${SEQUENCE}" > "${SEQ_FILE}"
    fi
    
    echo "序列文件: ${SEQ_FILE}"
    echo ""
    
    # 后台启动 - 用python包装脚本
    local LOG_FILE="${LOG_DIR}/batch_scheduler_$(date +%Y%m%d_%H%M%S).log"
    nohup python3 ${SCRIPT_DIR}/run_scheduler.py \
        --sequence-file "${SEQ_FILE}" \
        --data-size ${DATA_SIZE} \
        --workload ${WORKLOAD} \
        --interval-min ${INTERVAL_MIN} \
        --interval-max ${INTERVAL_MAX} \
        --max-batch-size ${MAX_BATCH_SIZE} \
        --output-dir "${DATA_DIR}" \
        > "${LOG_FILE}" 2>&1 &
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
    python3 /project/data/data_scripts/collect_data/enhanced_status.py
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
    echo "  TOTAL_RUNS              总任务数 (默认: 5000)"
    echo "  NORMAL_RATIO            正常任务比例 (默认: 0.898)"
    echo "  FAULT_TYPES             故障类型，逗号分隔 (默认: 14种，无permission_denied)"
    echo "  INTERVAL_MIN            最小间隔秒数 (默认: 30)"
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
    echo "  TOTAL_RUNS=100 NORMAL_RATIO=0.9 FAULT_TYPES='data_skew,task_fail' WORKLOAD=random $0 start"
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
