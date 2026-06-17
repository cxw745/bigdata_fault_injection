#!/usr/bin/env python3
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import signal
import atexit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def stop_datanode(node):
    try:
        run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode"')
        print(f"  ✔ DataNode on {node} 已停止")
        return True
    except Exception as e:
        print(f"  ⚠ 停止DataNode失败: {e}")
        try:
            run(f'ssh {node} "sudo pkill -f org.apache.hadoop.hdfs.server.datanode.DataNode"')
            print(f"  ✔ 通过pkill停止DataNode on {node}")
            return True
        except Exception as e2:
            print(f"  ✘ pkill也失败: {e2}")
            return False

def start_datanode(node):
    try:
        run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
        print(f"  ✔ DataNode on {node} 已启动")
        return True
    except Exception as e:
        print(f"  ⚠ 启动DataNode失败: {e}")
        return False


# Global state for cleanup
_target_node_global = None
_dn_stopped = False
_cleanup_done = False

def do_cleanup():
    global _cleanup_done, _dn_stopped
    if _cleanup_done:
        return
    _cleanup_done = True
    if _dn_stopped and _target_node_global:
        print(f'Restoring DataNode on {_target_node_global}...')
        start_datanode(_target_node_global)
        time.sleep(15)
        # Verify restart
        for retry in range(3):
            try:
                check = run(f'ssh {_target_node_global} "jps | grep DataNode | wc -l"')
                if check.strip() != "0":
                    print(f'  DataNode on {_target_node_global} confirmed running')
                    break
                print(f'  Retry {retry+1}: DataNode not running, restarting...')
                start_datanode(_target_node_global)
                time.sleep(15)
            except:
                pass

def signal_handler(signum, frame):
    print(f'\nWARNING: Signal received, cleaning up...')
    do_cleanup()
    sys.exit(130)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("心跳超时故障注入")
print("=" * 60)

FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "120"))
TARGET_NODE = os.environ.get("TARGET_NODE", "cxw-2")

print(f"\n▶ 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  目标节点: {TARGET_NODE}")
print(f"  方法: 停止DataNode进程，等待NameNode检测到心跳超时")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/heartbeat_timeout_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
mark_fault_start("heartbeat_timeout", {
    "duration": FAULT_DURATION,
    "target_node": TARGET_NODE,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="heartbeat_timeout_fault" \
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
        result = run("yarn application -list 2>/dev/null")
        for line in result.split("\n"):
            if "heartbeat_timeout_fault" in line and "RUNNING" in line:
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

try:
    print(f"\n▶ 注入故障: 停止DataNode on {TARGET_NODE}")
    _target_node_global = TARGET_NODE; _dn_stopped = True; stop_datanode(TARGET_NODE)
    mark_fault_injection("heartbeat_timeout", TARGET_NODE, "stop_datanode", FAULT_DURATION)

    print(f"\n▶ 等待故障持续时间: {FAULT_DURATION}s...")
    print(f"  (NameNode将在约30秒后标记{TARGET_NODE}为DEAD)")
    time.sleep(FAULT_DURATION)
finally:
    print(f"\n▶ 恢复故障: 重启DataNode on {TARGET_NODE}")
    start_datanode(TARGET_NODE)
    print(f"  ✔ DataNode已重启，等待重新注册...")
    time.sleep(15)
    # Verify DataNode is actually running after restart (with retry loop)
    for retry in range(3):
        try:
            check = run(f'ssh {TARGET_NODE} "jps | grep DataNode | wc -l"')
            if check.strip() != "0":
                print(f"  DataNode on {TARGET_NODE} confirmed running (attempt {retry+1})")
                break
            print(f"  WARNING: DataNode not running (attempt {retry+1}/3), restarting...")
            start_datanode(TARGET_NODE)
            time.sleep(15)
        except Exception as e:
            print(f"  WARNING: DataNode check failed: {e}")
            time.sleep(10)

    # NameNode-side verification
    try:
        report = run("/opt/hadoop/bin/hdfs dfsadmin -report 2>/dev/null | grep -A2 " + TARGET_NODE)
        if "Dead" in report or not report.strip():
            print(f"  WARNING: DataNode {TARGET_NODE} may not be registered with NameNode yet")
        else:
            print(f"  DataNode {TARGET_NODE} registered with NameNode")
    except:
        pass


print("\n▶ 等待任务完成...")
stdout_lines = []
for line in iter(process.stdout.readline, ""):
    stdout_lines.append(line)
    print(line, end="")

try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

if process.returncode == 0:
    print("\n✔ 任务完成")
    _cleanup_done = True
    mark_fault_end("heartbeat_timeout", {"result": "success"})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    _cleanup_done = True
    mark_fault_end("heartbeat_timeout", {"result": "completed_with_failures"})

print("\n" + "=" * 60)
print("🎉 心跳超时故障注入完成")
print("=" * 60)
