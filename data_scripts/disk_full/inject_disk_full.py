#!/usr/bin/env python3
"""
磁盘空间耗尽故障注入 (v2 - 健壮恢复版)

修复说明：
1. 添加signal handler和atexit保护，异常中断时也能恢复
2. 修复清理顺序：先blade destroy，再兜底rm残留文件
3. 移除硬编码sudo密码，使用sudo免密
4. eval()改为json.loads()解析ChaosBlade返回值
5. dd填充改为同步执行，确保填充完成
6. 残留文件通配符改为/chaos_filldisk*覆盖所有分片
7. 恢复后增加DataNode健康检查和自动重启
8. 恢复后增加磁盘使用率验证
"""
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import json
import re
import signal
import atexit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHAOSBLADE_PATH = "/opt/chaosblade-1.7.2/blade"

# 全局状态
_chaosblade_uid = None
_use_fallback = False
_target_node = None
_cleanup_done = False

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as e:
        return e.output.strip() if e.output else ""

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def check_chaosblade_available(node=None):
    cmd = f"test -x {CHAOSBLADE_PATH} && echo ok"
    if node:
        cmd = f"ssh {node} \"{cmd}\""
    try:
        result = run(cmd)
        return "ok" in result
    except:
        return False

def get_disk_usage_percent(node):
    try:
        result = run(f"ssh {node} \"df / | tail -1 | awk '{{print \\$5}}' | tr -d '%'\"")
        return int(result)
    except:
        return 0

def parse_chaosblade_uid(result_str):
    """安全解析ChaosBlade返回的UID"""
    # 方法1: json.loads()
    try:
        data = json.loads(result_str)
        if isinstance(data, dict) and "result" in data:
            return data["result"]
    except (json.JSONDecodeError, ValueError):
        pass
    # 方法2: 正则匹配
    match = re.search(r'"result"\s*:\s*"([^"]+)"', result_str)
    if match:
        return match.group(1)
    # 方法3: 匹配uid字段
    match = re.search(r'"uid"\s*:\s*"([^"]+)"', result_str)
    if match:
        return match.group(1)
    return None

def check_datanode_health(node):
    """检查DataNode健康状态，不健康则重启"""
    try:
        dn_check = run(f'ssh {node} "jps | grep DataNode | wc -l"')
        if dn_check.strip() == "0":
            print(f"  WARNING: DataNode on {node} not running, restarting...")
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
            time.sleep(2)
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            time.sleep(10)
            # 二次验证
            dn_check2 = run(f'ssh {node} "jps | grep DataNode | wc -l"')
            if dn_check2.strip() == "0":
                # fallback重启
                run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
                time.sleep(3)
                run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
                time.sleep(15)
            print(f"  DataNode on {node} restarted")
        else:
            print(f"  DataNode on {node} running normally")
    except Exception as e:
        print(f"  WARNING: DataNode check failed on {node}: {e}")
        try:
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
            time.sleep(2)
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            time.sleep(15)
            print(f"  DataNode on {node} restarted (fallback)")
        except Exception as e2:
            print(f"  ERROR: DataNode restart failed on {node}: {e2}")

def do_cleanup():
    """执行完整的恢复清理逻辑"""
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True

    print("\n> 执行恢复清理...")

    if _target_node:
        # 1. 先销毁ChaosBlade规则（让它自行清理填充文件）
        if _chaosblade_uid:
            print(f"  销毁ChaosBlade规则 (UID: {_chaosblade_uid})...")
            try:
                result = run(f'ssh {_target_node} "sudo {CHAOSBLADE_PATH} destroy {_chaosblade_uid}"')
                print(f"  销毁结果: {result}")
            except Exception as e:
                print(f"  WARNING: ChaosBlade销毁失败: {e}")

        # 2. 兜底清理残留文件（ChaosBlade可能产生的所有文件）
        print("  清理残留文件...")
        try:
            run(f'ssh {_target_node} "sudo rm -f /chaos_filldisk* /chaos_burnio.read /chaos_burnio.write /tmp/disk_fill_stress /tmp/disk_stress 2>/dev/null || true"')
            print("  残留文件已清理")
        except Exception as e:
            print(f"  WARNING: 残留文件清理失败: {e}")

        # 3. 清理dd降级方案
        if _use_fallback:
            print("  清理dd降级方案...")
            try:
                run(f'ssh {_target_node} "sudo pkill -f \'dd if=/dev/zero of=/tmp/disk_fill_stress\' 2>/dev/null || true"')
                run(f'ssh {_target_node} "sudo rm -f /tmp/disk_fill_stress 2>/dev/null || true"')
                print("  dd进程已终止，临时文件已清理")
            except Exception as e:
                print(f"  WARNING: dd清理失败: {e}")

        # 4. 验证磁盘使用率恢复
        recovered_usage = get_disk_usage_percent(_target_node)
        print(f"  恢复后磁盘使用率: {recovered_usage}%")

        # 5. DataNode健康检查
        check_datanode_health(_target_node)

def signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name
    print(f"\nWARNING: 收到信号 {sig_name}，执行清理...")
    do_cleanup()
    sys.exit(130)

# 注册signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

# ===== 主逻辑 =====
print("=" * 60)
print("磁盘空间耗尽故障注入 (v2 - 健壮恢复版)")
print("=" * 60)

FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "60"))
TARGET_NODE = os.environ.get("TARGET_NODE", "cxw-2")
FILL_PERCENT = int(os.environ.get("FILL_PERCENT", "90"))
_target_node = TARGET_NODE

print(f"\n> 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  目标节点: {TARGET_NODE}")
print(f"  填充目标: {FILL_PERCENT}%")

current_usage = get_disk_usage_percent(TARGET_NODE)
print(f"  当前磁盘使用率: {current_usage}%")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/disk_full_output"

print(f"\n> 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n> 启动 MapReduce 任务...")
mark_fault_start("disk_full", {
    "duration": FAULT_DURATION,
    "target_node": TARGET_NODE,
    "fill_percent": FILL_PERCENT,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="disk_full_fault" \\
    -D mapreduce.job.maps=24 \\
    -D mapreduce.job.reduces=8 \\
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \\
    -input {input_path} \\
    -output {output_path} \\
    -mapper "python3 mapper.py" \\
    -reducer "python3 reducer.py" \\
    -file {mapper_path} \\
    -file {reducer_path}
"""

process = run_background(cmd)

print("\n> 等待任务启动...")
time.sleep(10)

print("\n> 查找运行中的应用...")
application_id = None
for attempt in range(30):
    try:
        result = run("yarn application -list 2>/dev/null")
        for line in result.split("\n"):
            if "disk_full_fault" in line and "RUNNING" in line:
                parts = line.split()
                for part in parts:
                    if part.startswith("application_"):
                        application_id = part
                        break
                if application_id:
                    break
    except Exception:
        pass
    if application_id:
        break
    time.sleep(5)

if application_id:
    print(f"  Found application: {application_id}")
    print(f"application_{application_id.split('_', 1)[1] if '_' in application_id else application_id}")
else:
    print("  WARNING: 未找到应用ID，继续执行故障注入")

# ===== 注入故障 =====
print(f"\n> 注入故障: 磁盘空间填充至{FILL_PERCENT}% on {TARGET_NODE}")

if check_chaosblade_available(TARGET_NODE):
    print(f"  尝试ChaosBlade disk fill...")
    try:
        result = run(f'ssh {TARGET_NODE} "sudo {CHAOSBLADE_PATH} create disk fill --percent {FILL_PERCENT}"')
        print(f"  ChaosBlade响应: {result}")
        _chaosblade_uid = parse_chaosblade_uid(result)
        if _chaosblade_uid:
            print(f"  ChaosBlade UID: {_chaosblade_uid}")
            new_usage = get_disk_usage_percent(TARGET_NODE)
            print(f"  当前磁盘使用率: {new_usage}%")
            mark_fault_injection("disk_full", TARGET_NODE, f"chaosblade_disk_fill_{FILL_PERCENT}pct", FAULT_DURATION)
        else:
            print("  WARNING: 未能获取ChaosBlade UID，尝试降级方案")
            _use_fallback = True
    except Exception as e:
        print(f"  WARNING: ChaosBlade不可用: {e}")
        print("  使用dd降级方案...")
        _use_fallback = True
else:
    print(f"  ChaosBlade不可用 on {TARGET_NODE}，使用dd降级方案")
    _use_fallback = True

if _use_fallback:
    print(f"  降级方案: dd填充磁盘空间 on {TARGET_NODE}")
    try:
        disk_info = run(f"ssh {TARGET_NODE} \"df / | tail -1\"").split()
        total_kb = int(disk_info[1])
        used_kb = int(disk_info[2])
        target_used_kb = int(total_kb * FILL_PERCENT / 100)
        fill_kb = target_used_kb - used_kb
        if fill_kb > 0:
            fill_mb = fill_kb // 1024
            print(f"  需要填充: {fill_mb}MB (当前{used_kb//1024}MB/{total_kb//1024}MB -> 目标{FILL_PERCENT}%)")
            # 同步dd填充（不加&），确保填充完成
            run(f'ssh {TARGET_NODE} "sudo dd if=/dev/zero of=/tmp/disk_fill_stress bs=1M count={fill_mb} 2>/dev/null"')
            new_usage = get_disk_usage_percent(TARGET_NODE)
            print(f"  dd填充完成，当前使用率: {new_usage}%")
            mark_fault_injection("disk_full", TARGET_NODE, f"dd_fill_{fill_mb}MB", FAULT_DURATION)
        else:
            print(f"  磁盘已超过{FILL_PERCENT}%，无需填充")
    except Exception as e:
        print(f"  WARNING: dd降级方案失败: {e}")

print(f"\n> 等待故障持续时间: {FAULT_DURATION}s...")
time.sleep(FAULT_DURATION)

# ===== 恢复故障 =====
print(f"\n> 恢复故障: 释放磁盘空间")

# 1. 先销毁ChaosBlade规则（让它自行清理填充文件）
if _chaosblade_uid:
    print(f"  销毁ChaosBlade规则 (UID: {_chaosblade_uid})...")
    try:
        result = run(f'ssh {TARGET_NODE} "sudo {CHAOSBLADE_PATH} destroy {_chaosblade_uid}"')
        print(f"  销毁结果: {result}")
    except Exception as e:
        print(f"  WARNING: ChaosBlade销毁失败: {e}")

# 2. 兜底清理残留文件
print("  清理残留文件...")
try:
    run(f'ssh {TARGET_NODE} "sudo rm -f /chaos_filldisk* /chaos_burnio.read /chaos_burnio.write /tmp/disk_fill_stress /tmp/disk_stress 2>/dev/null || true"')
    print("  残留文件已清理")
except Exception as e:
    print(f"  WARNING: 残留文件清理失败: {e}")

# 3. 清理dd降级方案
if _use_fallback:
    print("  清理dd降级方案...")
    try:
        run(f'ssh {TARGET_NODE} "sudo pkill -f \'dd if=/dev/zero of=/tmp/disk_fill_stress\' 2>/dev/null || true"')
        run(f'ssh {TARGET_NODE} "sudo rm -f /tmp/disk_fill_stress 2>/dev/null || true"')
        print("  dd进程已终止，临时文件已清理")
    except Exception as e:
        print(f"  WARNING: dd清理失败: {e}")

# 标记清理完成，防止atexit重复执行
_cleanup_done = True

# 4. 验证磁盘使用率恢复
recovered_usage = get_disk_usage_percent(TARGET_NODE)
print(f"  恢复后磁盘使用率: {recovered_usage}%")

# 5. DataNode健康检查
print("\n> 检查DataNode健康状态...")
check_datanode_health(TARGET_NODE)

mark_fault_end("disk_full", {"result": "recovery_completed"})

print("\n> 等待任务完成...")
stdout_lines = []
for line in iter(process.stdout.readline, ""):
    stdout_lines.append(line)
    print(line, end="")

try:
    process.wait(timeout=600)
except subprocess.TimeoutExpired:
    print("\nWARNING: MapReduce任务超时(600s)，强制终止")
    process.kill()
    process.wait(timeout=5)

if process.returncode == 0:
    print("\n> 任务完成")
    mark_fault_end("disk_full", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n> 任务返回码: {process.returncode}")
    mark_fault_end("disk_full", {"result": "completed_with_failures", "returncode": process.returncode})

print("\n> 查看输出结果...")
try:
    result = run(f"{hadoop_cmd} fs -ls {output_path} 2>/dev/null || true")
    if result:
        print("  输出目录内容:")
        for line in result.split("\n")[:5]:
            print(f"    {line}")
except Exception as e:
    print(f"  无法读取输出: {e}")

print("\n" + "=" * 60)
print("磁盘空间耗尽故障注入完成 (v2)")
print("=" * 60)
