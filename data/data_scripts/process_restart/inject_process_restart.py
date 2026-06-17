#!/usr/bin/env python3
"""
进程重启故障注入

在MapReduce任务运行期间，杀死并重启DataNode进程，
模拟节点服务中断场景
"""
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



# Global state for cleanup
_target_node_global = None
_stopped_service = None
_cleanup_done = False

def do_cleanup():
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True
    if _target_node_global and _stopped_service:
        print(f'Restoring {_stopped_service} on {_target_node_global}...')
        try:
            run(f'ssh {_target_node_global} "/opt/hadoop/bin/hdfs --daemon start {_stopped_service}"')
            time.sleep(15)
            # Verify
            check = run(f'ssh {_target_node_global} "jps | grep -i {_stopped_service.replace("_", "")} | wc -l"')
            if check.strip() == "0":
                run(f'ssh {_target_node_global} "/opt/hadoop/bin/hdfs --daemon start {_stopped_service}"')
                time.sleep(10)
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
print("进程重启故障注入")
print("=" * 60)

_target_node_global = TARGET_NODE = os.environ.get("TARGET_NODE", "cxw-2")
_stopped_service = TARGET_SERVICE = os.environ.get("TARGET_SERVICE", "datanode")
RESTART_DELAY = int(os.environ.get("RESTART_DELAY", "10"))

print(f"\n▶ 故障参数:")
print(f"  目标节点: {TARGET_NODE}")
print(f"  目标服务: {TARGET_SERVICE}")
print(f"  重启延迟: {RESTART_DELAY}s")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/process_restart_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

mark_fault_start("process_restart", {
    "target_node": TARGET_NODE,
    "target_service": TARGET_SERVICE,
    "restart_delay": RESTART_DELAY,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="process_restart_fault" \
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
            if "process_restart_fault" in line and "RUNNING" in line:
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
    print(f"  找到应用: {application_id}")
else:
    print("  ⚠ 未找到应用ID，继续执行故障注入")

print(f"\n▶ 注入故障: 停止 {TARGET_SERVICE} on {TARGET_NODE}")
try:
    result = run(f'ssh {TARGET_NODE} "/opt/hadoop/bin/hdfs --daemon stop {TARGET_SERVICE}"')
    print(f"  停止结果: {result if result else 'OK'}")
    mark_fault_injection("process_restart", f"{TARGET_NODE}:{TARGET_SERVICE}", "stop_service", RESTART_DELAY)
except Exception as e:
    print(f"  ⚠ 停止服务失败: {e}")

print(f"\n▶ 等待重启延迟: {RESTART_DELAY}s...")
time.sleep(RESTART_DELAY)

print(f"\n▶ 恢复故障: 启动 {TARGET_SERVICE} on {TARGET_NODE}")
try:
    result = run(f'ssh {TARGET_NODE} "/opt/hadoop/bin/hdfs --daemon start {TARGET_SERVICE}"')
    print(f"  启动结果: {result if result else 'OK'}")
    mark_fault_injection("process_restart", f"{TARGET_NODE}:{TARGET_SERVICE}", "start_service", None)
except Exception as e:
    print(f"  ⚠ 启动服务失败: {e}")

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
    
    # Verify DataNode health after restart
    if _stopped_service == "datanode":
        try:
            for retry in range(3):
                check = run(f'ssh {_target_node_global} "jps | grep DataNode | wc -l"')
                if check.strip() != "0":
                    print(f"  DataNode on {_target_node_global} confirmed running")
                    break
                print(f"  Retry {retry+1}: DataNode not running...")
                run(f'ssh {_target_node_global} "/opt/hadoop/bin/hdfs --daemon start datanode"')
                time.sleep(15)
        except Exception as e:
            print(f"  WARNING: DataNode health check failed: {e}")

    _cleanup_done = True
    mark_fault_end("process_restart", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    
# Verify DataNode health after restart
if _stopped_service == "datanode":
    try:
        for retry in range(3):
            check = run(f'ssh {_target_node_global} "jps | grep DataNode | wc -l"')
            if check.strip() != "0":
                print(f"  DataNode on {_target_node_global} confirmed running")
                break
            print(f"  Retry {retry+1}: DataNode not running...")
            run(f'ssh {_target_node_global} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            time.sleep(15)
    except Exception as e:
        print(f"  WARNING: DataNode health check failed: {e}")

_cleanup_done = True
mark_fault_end("process_restart", {"result": "completed_with_failures", "returncode": process.returncode})

print("\n▶ 查看输出结果...")
try:
    result = run(f"{hadoop_cmd} fs -ls {output_path} 2>/dev/null || true")
    if result:
        print("  输出目录内容:")
        for line in result.split("\n")[:5]:
            print(f"    {line}")
except Exception as e:
    print(f"  无法读取输出: {e}")

print("\n" + "=" * 60)
print("🎉 进程重启故障注入完成")
print("=" * 60)
