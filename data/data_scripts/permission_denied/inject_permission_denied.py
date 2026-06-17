#!/usr/bin/env python3
"""
权限拒绝故障注入 (v3 - 健壮恢复版)

修复说明：
- v3修复：
  1. 修正HiBench prepare脚本路径为 /opt/HiBench/bin/workloads/micro/wordcount/prepare/prepare.sh
  2. 恢复时先删除输出目录再重新生成数据
  3. 设置正确的环境变量 HIBENCH_HOME 和 HADOOP_HOME
  4. 如果prepare失败，打印明确的修复指引
  5. 增加signal handler
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
FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "120"))
INJECT_DELAY = int(os.environ.get("INJECT_DELAY", "20"))

HIBENCH_HOME = "/opt/HiBench"
HADOOP_HOME = "/opt/hadoop"
HIBENCH_PREPARE_PATH = f"{HIBENCH_HOME}/bin/workloads/micro/wordcount/prepare/prepare.sh"

# 全局状态
_deleted_files = []
_process = None
_cleanup_done = False

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as e:
        return e.output.strip() if e.output else ""

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def do_cleanup():
    """执行完整的恢复清理逻辑"""
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True

    print("\n▶ 执行恢复清理...")
    hadoop_cmd = f"{HADOOP_HOME}/bin/hadoop"
    input_path = "/HiBench/HiBench/Wordcount/Input"

    # 1. 确保权限正确
    try:
        run(f"{hadoop_cmd} dfs -chmod -R 755 {input_path}/ 2>/dev/null || true")
        print("  ✔ 权限已确保为755")
    except:
        pass

    # 2. 重新生成输入数据
    print("  重新生成输入数据...")
    try:
        # 先删除输出目录（如果存在），避免prepare失败
        run(f"{hadoop_cmd} fs -rm -r /user/hadoop/permission_denied_output 2>/dev/null || true")

        # 设置环境变量后运行prepare
        env_cmd = f"export HIBENCH_HOME={HIBENCH_HOME} && export HADOOP_HOME={HADOOP_HOME} && cd {HIBENCH_HOME} && {HIBENCH_PREPARE_PATH}"
        result = run(env_cmd)
        if "Error" in result or "error" in result.lower():
            print(f"  ⚠ prepare可能有错误: {result[-200:]}")
        else:
            print("  ✔ 输入数据已重新生成")
    except Exception as e:
        print(f"  ⚠ 输入数据重新生成失败: {e}")
        print(f"  手动修复步骤:")
        print(f"    1. export HIBENCH_HOME={HIBENCH_HOME}")
        print(f"    2. export HADOOP_HOME={HADOOP_HOME}")
        print(f"    3. cd {HIBENCH_HOME}")
        print(f"    4. {HIBENCH_PREPARE_PATH}")

def verify_recovery():
    """验证恢复结果"""
    print("\n▶ 验证恢复结果...")
    hadoop_cmd = f"{HADOOP_HOME}/bin/hadoop"
    input_path = "/HiBench/HiBench/Wordcount/Input"

    try:
        result = run(f"{hadoop_cmd} dfs -ls {input_path}/ 2>/dev/null")
        file_count = len([l for l in result.split('\n') if l.strip() and l.split()[-1].startswith('/')])
        if file_count > 0:
            print(f"  ✔ 输入文件已恢复: {file_count} 个文件")
        else:
            print(f"  ✘ 输入文件未恢复，需要手动运行prepare")
            print(f"    手动修复: cd {HIBENCH_HOME} && {HIBENCH_PREPARE_PATH}")
    except Exception as e:
        print(f"  ⚠ 验证失败: {e}")

def signal_handler(signum, frame):
    """处理中断信号"""
    sig_name = signal.Signals(signum).name
    print(f"\n⚠ 收到信号 {sig_name}，执行清理...")
    do_cleanup()
    sys.exit(130)

# 注册signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("权限拒绝故障注入 (v3 - 健壮恢复版)")
print("=" * 60)

hadoop_cmd = f"{HADOOP_HOME}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/permission_denied_output"

print(f"\n▶ 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  注入延迟: {INJECT_DELAY}s")
print(f"  目标路径: {input_path}")

# 记录原始权限和属主
print(f"\n▶ 记录原始权限...")
try:
    orig_perm = run(f"{hadoop_cmd} dfs -ls -d {input_path} 2>/dev/null")
    print(f"  原始: {orig_perm}")
except:
    print("  ⚠ 无法获取原始权限")

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
mark_fault_start("permission_denied", {
    "duration": FAULT_DURATION,
    "target_path": input_path,
    "inject_delay": INJECT_DELAY,
})

cmd = f"""
{hadoop_cmd} jar {HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="permission_denied_fault" \\
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

_process = run_background(cmd)

print(f"\n▶ 等待任务充分运行 ({INJECT_DELAY}s)...")
time.sleep(INJECT_DELAY)

print("\n▶ 查找运行中的应用...")
application_id = None
for attempt in range(30):
    try:
        result = run("yarn application -list 2>/dev/null")
        for line in result.split("\n"):
            if "permission_denied_fault" in line and "RUNNING" in line:
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

# ===== 核心修复v3：删除部分输入文件，触发FileNotFoundException =====
print(f"\n▶ 注入故障: 删除部分输入文件 (模拟权限拒绝导致文件不可访问)")

try:
    files_str = run(f"{hadoop_cmd} dfs -ls {input_path}/ 2>/dev/null")
    file_list = [l.split()[-1] for l in files_str.split('\n') if l.strip() and l.split()[-1].startswith('/')]
    print(f"  找到 {len(file_list)} 个输入文件")

    files_to_delete = file_list[:len(file_list)//2]
    for f in files_to_delete:
        run(f"{hadoop_cmd} dfs -rm {f} 2>/dev/null || true")
        _deleted_files.append(f)
    print(f"  已删除 {len(_deleted_files)} 个输入文件: {[f.split('/')[-1] for f in _deleted_files]}")
    mark_fault_injection("permission_denied", input_path, "delete_input_files", FAULT_DURATION)
except Exception as e:
    print(f"  ⚠ 删除文件失败: {e}")

print(f"\n▶ 等待故障持续时间: {FAULT_DURATION}s...")
try:
    time.sleep(FAULT_DURATION)
except KeyboardInterrupt:
    print("\n  收到中断信号，提前终止...")

# ===== 恢复故障 =====
print(f"\n▶ 恢复故障: 重新生成被删除的输入文件...")

# 先删除输出目录（如果存在），避免prepare因输出目录已存在而失败
try:
    run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")
    print("  ✔ 输出目录已清理")
except:
    pass

# 确保权限正确
try:
    run(f"{hadoop_cmd} dfs -chmod -R 755 {input_path}/ 2>/dev/null || true")
    print("  ✔ 权限已确保为755")
except:
    pass

# 重新生成输入数据（使用正确的HiBench prepare脚本路径）
print("  重新生成输入数据...")
try:
    env_cmd = f"export HIBENCH_HOME={HIBENCH_HOME} && export HADOOP_HOME={HADOOP_HOME} && cd {HIBENCH_HOME} && {HIBENCH_PREPARE_PATH}"
    result = run(env_cmd)
    if "Error" in result or "error" in result.lower():
        print(f"  ⚠ prepare可能有错误: {result[-200:]}")
    else:
        print("  ✔ 输入数据已重新生成")
except Exception as e:
    print(f"  ⚠ 输入数据重新生成失败: {e}")
    print(f"  手动修复步骤:")
    print(f"    1. export HIBENCH_HOME={HIBENCH_HOME}")
    print(f"    2. export HADOOP_HOME={HADOOP_HOME}")
    print(f"    3. cd {HIBENCH_HOME}")
    print(f"    4. {HIBENCH_PREPARE_PATH}")

# 标记清理完成，防止atexit重复执行
_cleanup_done = True

# 验证恢复
verify_recovery()

mark_fault_end("permission_denied", {"result": "recovery_completed"})

print("\n▶ 等待任务完成...")
stdout_lines = []
for line in iter(_process.stdout.readline, ""):
    stdout_lines.append(line)
    print(line, end="")

try:
    _process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    _process.kill()
    _process.wait(timeout=5)

if _process.returncode == 0:
    print("\n✔ 任务完成")
    mark_fault_end("permission_denied", {"result": "success", "returncode": _process.returncode})
else:
    print(f"\n⚠ 任务返回码: {_process.returncode} (权限拒绝导致任务失败，属于预期行为)")
    mark_fault_end("permission_denied", {"result": "fault_injected_successfully_job_failed_as_expected", "returncode": _process.returncode})

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
print("权限拒绝故障注入完成 (v3)")
print("=" * 60)
