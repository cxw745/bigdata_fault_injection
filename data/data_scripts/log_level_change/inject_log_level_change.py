#!/usr/bin/env python3
"""
日志级别变更故障注入

在MapReduce任务运行期间，通过JMX/HTTP API
将Hadoop组件日志级别从INFO改为DEBUG，
导致大量日志输出影响性能
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
_cleanup_done = False

def do_cleanup():
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True
    if _target_node_global:
        # Restore log levels
        try:
            run(f'curl -s "http://{_target_node_global}:{DATANODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=INFO"')
        except:
            pass
        try:
            run(f'curl -s "http://cxw-1:{NAMENODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=INFO"')
        except:
            pass
        print("  Log levels restored to INFO")

def signal_handler(signum, frame):
    print(f'\nWARNING: Signal received, cleaning up...')
    do_cleanup()
    sys.exit(130)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("日志级别变更故障注入")
print("=" * 60)

FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "60"))
_target_node_global = TARGET_NODE = os.environ.get("TARGET_NODE", "cxw-2")
DATANODE_HTTP_PORT = 9864
NAMENODE_HTTP_PORT = 9870

print(f"\n▶ 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  目标节点: {TARGET_NODE}")
print(f"  DataNode端口: {DATANODE_HTTP_PORT}")
print(f"  NameNode端口: {NAMENODE_HTTP_PORT}")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/log_level_change_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

mark_fault_start("log_level_change", {
    "duration": FAULT_DURATION,
    "target_node": TARGET_NODE,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="log_level_change_fault" \
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
            if "log_level_change_fault" in line and "RUNNING" in line:
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

print(f"\n▶ 注入故障: 修改日志级别 INFO -> DEBUG")
print(f"  DataNode ({TARGET_NODE}:{DATANODE_HTTP_PORT})...")
try:
    result = run(f'curl -s "http://{TARGET_NODE}:{DATANODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=DEBUG"')
    print(f"  DataNode响应: {result[:100]}")
    mark_fault_injection("log_level_change", f"{TARGET_NODE}:{DATANODE_HTTP_PORT}", "change_log_level_to_DEBUG", FAULT_DURATION)
except Exception as e:
    print(f"  ⚠ DataNode日志级别修改失败: {e}")

print(f"  NameNode (cxw-1:{NAMENODE_HTTP_PORT})...")
try:
    result = run(f'curl -s "http://cxw-1:{NAMENODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=DEBUG"')
    print(f"  NameNode响应: {result[:100]}")
    mark_fault_injection("log_level_change", f"cxw-1:{NAMENODE_HTTP_PORT}", "change_log_level_to_DEBUG", FAULT_DURATION)
except Exception as e:
    print(f"  ⚠ NameNode日志级别修改失败: {e}")

print(f"\n▶ 等待故障持续时间: {FAULT_DURATION}s...")
time.sleep(FAULT_DURATION)

print(f"\n▶ 恢复故障: 修改日志级别 DEBUG -> INFO")
print(f"  DataNode ({TARGET_NODE}:{DATANODE_HTTP_PORT})...")
try:
    result = run(f'curl -s "http://{TARGET_NODE}:{DATANODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=INFO"')
    print(f"  DataNode响应: {result[:100]}")
except Exception as e:
    print(f"  ⚠ DataNode日志级别恢复失败: {e}")

print(f"  NameNode (cxw-1:{NAMENODE_HTTP_PORT})...")
try:
    result = run(f'curl -s "http://cxw-1:{NAMENODE_HTTP_PORT}/logLevel?log=org.apache.hadoop&level=INFO"')
    print(f"  NameNode响应: {result[:100]}")
except Exception as e:
    print(f"  ⚠ NameNode日志级别恢复失败: {e}")

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
    mark_fault_end("log_level_change", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("log_level_change", {"result": "completed_with_failures", "returncode": process.returncode})

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
print("🎉 日志级别变更故障注入完成")
print("=" * 60)
