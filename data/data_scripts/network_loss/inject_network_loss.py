#!/usr/bin/env python3
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import json
import re
import signal
import atexit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collect_data.unified_config import get_slave_nodes, get_master_node, DEFAULT_FAULT_PARAMS
from collect_data.fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

CHAOSBLADE_PATH = "/opt/chaosblade-1.7.2/blade"
FAULT_DURATION = DEFAULT_FAULT_PARAMS.get("fault_duration", 60)
LOSS_PERCENT = int(os.environ.get("LOSS_PERCENT", "30"))
NETWORK_INTERFACE = "ens3"

# Global state for cleanup
_cleanup_done = False
_chaosblade_uid_global = None
_target_node_global = None
_interface_global = None

def run(cmd, check=True):
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=check
        )
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return e.stdout.strip() if e.stdout else "", e.stderr.strip() if e.stderr else ""

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def get_network_interface(node=None):
    if node:
        stdout, _ = run(f"ssh {node} \"ip route | head -1 | awk '{{print \\$5}}'\"", check=False)
        if stdout.strip():
            return stdout.strip()
    stdout, _ = run("ip route | head -1 | awk '{print $5}'", check=False)
    return stdout.strip() or "ens3"


def do_cleanup():
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True
    if _target_node_global and _chaosblade_uid_global:
        run(f'ssh {_target_node_global} "sudo {CHAOSBLADE_PATH} destroy {_chaosblade_uid_global}"', check=False)
    if _target_node_global and _interface_global:
        run(f'ssh {_target_node_global} "sudo tc qdisc del dev {_interface_global} root 2>/dev/null || true"', check=False)
        run(f'ssh {_target_node_global} "sudo iptables -F 2>/dev/null || true"', check=False)

def signal_handler(signum, frame):
    print(f'\nWARNING: Signal received, cleaning up...')
    do_cleanup()
    sys.exit(130)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("网络丢包故障注入 (network_loss)")
print("=" * 60)

TARGET_NODE = os.environ.get("TARGET_NODE", "cxw-2")
interface = get_network_interface(TARGET_NODE)

print(f"\n▶ 故障参数:")
print(f"  丢包率: {LOSS_PERCENT}%")
print(f"  目标节点: {TARGET_NODE}")
print(f"  网卡: {interface}")
print(f"  故障持续时间: {FAULT_DURATION}s")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/network_loss_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "common_mapreduce", "mapper.py")
reducer_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
mark_fault_start("network_loss", {
    "loss_percent": LOSS_PERCENT,
    "target_node": TARGET_NODE,
    "interface": interface,
    "duration": FAULT_DURATION,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="network_loss_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file {mapper_path} \
    -file {reducer_path}
"""

process = run_background(cmd)

print("\n▶ 等待任务启动...")
time.sleep(10)

print("\n▶ 查找运行中的应用...")
application_id = None
for attempt in range(30):
    try:
        result, _ = run("yarn application -list 2>/dev/null", check=False)
        for line in result.split("\n"):
            if "network_loss_fault" in line and "RUNNING" in line:
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
    print(f"  ✔ 找到应用: {application_id}")
    print(f"application_{application_id.split('_', 1)[1] if '_' in application_id else application_id}")
else:
    print("  ⚠ 未找到应用ID，继续执行故障注入")

chaosblade_uid = None
_target_node_global = TARGET_NODE
_interface_global = interface
use_tc_fallback = False

print(f"\n▶ 注入故障: {LOSS_PERCENT}%丢包 on {TARGET_NODE} ({interface})")

try:
    blade_cmd = f'ssh {TARGET_NODE} "sudo {CHAOSBLADE_PATH} create network loss --path to --percent {LOSS_PERCENT} --interface {interface} --exclude-port 22"'
    print(f"  执行: {blade_cmd}")
    result, err = run(blade_cmd, check=False)
    print(f"  ChaosBlade响应: {result}")
    for line in result.split("\n"):
        if '"uid"' in line:
            match = re.search(r'"uid"\s*:\s*"([^"]+)"', line)
            if match:
                chaosblade_uid = match.group(1)
                _chaosblade_uid_global = chaosblade_uid
            break
    if not chaosblade_uid:
        try:
            if "{" in result:
                import json as _json; result_json = _json.loads(result)
                if result_json and "uid" in result_json:
                    chaosblade_uid = result_json["uid"]
                    _chaosblade_uid_global = chaosblade_uid
        except Exception:
            pass
    if chaosblade_uid:
        print(f"  ✔ ChaosBlade UID: {chaosblade_uid}")
        mark_fault_injection("network_loss", TARGET_NODE, f"chaosblade_network_loss_{LOSS_PERCENT}pct", FAULT_DURATION)
    else:
        print("  ⚠ 未能获取ChaosBlade UID，尝试tc降级方案")
        use_tc_fallback = True
except Exception as e:
    print(f"  ⚠ ChaosBlade不可用: {e}")
    print("  使用tc降级方案...")
    use_tc_fallback = True

if use_tc_fallback:
    print(f"  降级方案: tc netem 丢包 on {TARGET_NODE}")
    try:
        run(f'ssh {TARGET_NODE} "sudo tc qdisc add dev {interface} root netem loss {LOSS_PERCENT}%"', check=False)
        print(f"  ✔ tc netem {LOSS_PERCENT}%丢包已注入")
        mark_fault_injection("network_loss", TARGET_NODE, f"tc_netem_loss_{LOSS_PERCENT}pct", FAULT_DURATION)
    except Exception as e:
        print(f"  ⚠ tc降级方案失败: {e}")

print(f"\n▶ 等待故障持续时间: {FAULT_DURATION}s...")
time.sleep(FAULT_DURATION)

print(f"\n▶ 恢复故障: 移除网络丢包规则")
if chaosblade_uid:
    print(f"  销毁ChaosBlade规则 (UID: {chaosblade_uid})...")
    try:
        result, _ = run(f'ssh {TARGET_NODE} "sudo {CHAOSBLADE_PATH} destroy {chaosblade_uid}"', check=False)
        print(f"  销毁结果: {result}")
    except Exception as e:
        print(f"  ⚠ ChaosBlade销毁失败: {e}")
        print("  尝试tc qdisc恢复...")
        run(f'ssh {TARGET_NODE} "sudo tc qdisc del dev {interface} root 2>/dev/null || true"', check=False)
elif use_tc_fallback:
    print(f"  清理tc netem规则...")
    try:
        run(f'ssh {TARGET_NODE} "sudo tc qdisc del dev {interface} root 2>/dev/null || true"', check=False)
        print("  ✔ tc规则已清理")
    except Exception as e:
        print(f"  ⚠ tc清理失败: {e}")

# Verify network recovery
if _target_node_global and _interface_global:
    tc_check, _ = run(f'ssh {_target_node_global} "sudo tc qdisc show dev {_interface_global} 2>/dev/null | grep netem || echo CLEAN"', check=False)
    if "CLEAN" in tc_check:
        print("  Network rules verified clean")
    else:
        print(f"  WARNING: Residual tc rules: {tc_check}")
        run(f'ssh {_target_node_global} "sudo tc qdisc del dev {_interface_global} root 2>/dev/null || true"', check=False)
        run(f'ssh {_target_node_global} "sudo iptables -F 2>/dev/null || true"', check=False)


# Verify DataNode health after network fault recovery
print("\n\u25b6 \u9a8c\u8bc1DataNode\u5065\u5eb7\u72b6\u6001...")
time.sleep(10)
try:
    dn_result, _ = run(f"ssh {TARGET_NODE} '/opt/hadoop/bin/hdfs --daemon status datanode 2>&1 || echo NOT_RUNNING'", check=False)
    if 'NOT_RUNNING' in dn_result or 'not running' in dn_result.lower():
        print(f"  \u26a0 DataNode on {TARGET_NODE} not running, restarting...")
        run(f"ssh {TARGET_NODE} '/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true'", check=False)
        time.sleep(2)
        run(f"ssh {TARGET_NODE} '/opt/hadoop/bin/hdfs --daemon start datanode'", check=False)
        print(f"  \u2714 DataNode on {TARGET_NODE} restarted")
        time.sleep(15)
    else:
        print(f"  \u2714 DataNode on {TARGET_NODE} running normally")
except Exception as e:
    print(f"  \u26a0 DataNode health check failed: {e}")
    try:
        run(f"ssh {TARGET_NODE} '/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true'", check=False)
        time.sleep(2)
        run(f"ssh {TARGET_NODE} '/opt/hadoop/bin/hdfs --daemon start datanode'", check=False)
        print(f"  \u2714 DataNode on {TARGET_NODE} restarted (fallback)")
        time.sleep(15)
    except Exception as e2:
        print(f"  \u2718 DataNode restart failed: {e2}")

print("\n▶ 等待任务完成...")
stdout_lines = []
for line in iter(process.stdout.readline, ""):
    stdout_lines.append(line)
    print(line, end="")

try:
    process.wait(timeout=600)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(600s)，强制终止")
    process.kill()
    process.wait(timeout=5)

if process.returncode == 0:
    print("\n✔ 任务完成")
    _cleanup_done = True
    mark_fault_end("network_loss", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    _cleanup_done = True
    mark_fault_end("network_loss", {"result": "completed_with_failures", "returncode": process.returncode})

print("\n▶ 查看输出结果...")
try:
    result, _ = run(f"{hadoop_cmd} fs -ls {output_path} 2>/dev/null || true", check=False)
    if result:
        print("  输出目录内容:")
        for line in result.split("\n")[:5]:
            print(f"    {line}")
except Exception as e:
    print(f"  无法读取输出: {e}")

print("\n" + "=" * 60)
print("🎉 网络丢包故障注入完成")
print("=" * 60)
