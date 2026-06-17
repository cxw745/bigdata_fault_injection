#!/usr/bin/env python3
"""
磁盘错误故障注入 (v3 - 健壮恢复版)

修复说明：
- v3修复：
  1. 恢复时使用sudo killall -9 dd确保所有dd进程被杀掉
  2. 恢复时使用sudo killall -9 chaos_os确保ChaosBlade进程被杀掉
  3. 修正目录权限恢复：使用chmod 700而不是chmod 755
  4. 增加signal handler
  5. 恢复时增加rm -f /tmp/disk_stress清理临时文件
  6. 增加恢复验证步骤
"""
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import re
import signal
import atexit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHAOSBLADE_PATH = "/opt/chaosblade-1.7.2/blade"
FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "120"))
INJECT_DELAY = int(os.environ.get("INJECT_DELAY", "15"))
TARGET_NODES = os.environ.get("TARGET_NODES", "cxw-2,cxw-3").split(",")

# 全局状态
_chaosblade_uids = []
_fallback_nodes = []
_dn_dirs_made_readonly = []
_cleanup_done = False

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as e:
        return e.output.strip() if e.output else ""

def run_bg(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def check_chaosblade_available(node=None):
    cmd = f"test -x {CHAOSBLADE_PATH} && echo 'ok'"
    if node:
        cmd = f"ssh {node} \"{cmd}\""
    try:
        result = run(cmd)
        return "ok" in result
    except:
        return False

def do_cleanup():
    """执行完整的恢复清理逻辑"""
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True

    print("\n▶ 执行恢复清理...")

    # 0. 清理ChaosBlade残留的大文件（兜底，防止磁盘被占满）
    for node in TARGET_NODES:
        try:
            run(f'ssh {node} "echo ubuntu | sudo -S rm -f /chaos_filldisk.log.dat /chaos_burnio.read /chaos_burnio.write /tmp/disk_stress /tmp/disk_fill_stress 2>/dev/null || true"')
            print(f"  ✔ ChaosBlade残留文件已清理 on {node}")
        except Exception as e:
            print(f"  ⚠ 清理残留文件失败 on {node}: {e}")

    # 1. 销毁ChaosBlade规则
    for node, uid in _chaosblade_uids:
        try:
            run(f'ssh {node} "echo ubuntu | sudo -S {CHAOSBLADE_PATH} destroy {uid}" 2>/dev/null || true')
            print(f"  ✔ ChaosBlade规则已销毁 on {node}")
        except:
            print(f"  ⚠ ChaosBlade销毁失败 on {node}")

    # 2. 杀掉所有相关进程（兜底清理）
    all_target_nodes = list(set([n for n, _ in _chaosblade_uids] + _fallback_nodes))
    for node in all_target_nodes:
        try:
            # Kill while-true loop first, then dd
            run(f'ssh {node} "echo ubuntu | sudo -S pkill -9 -f \"while true.*disk_stress\" 2>/dev/null || true"')
            time.sleep(1)
            run(f'ssh {node} "echo ubuntu | sudo -S killall -9 dd 2>/dev/null || true"')
            run(f'ssh {node} "echo ubuntu | sudo -S killall -9 chaos_os 2>/dev/null || true"')
            run(f'ssh {node} "echo ubuntu | sudo -S rm -f /tmp/disk_stress 2>/dev/null || true"')
            print(f"  ✔ 进程清理和临时文件删除完成 on {node}")
        except:
            pass

    # 3. 恢复DataNode目录权限
    for node, dn_dir in _dn_dirs_made_readonly:
        try:
            run(f'ssh {node} "echo ubuntu | sudo -S chmod 700 {dn_dir} 2>/dev/null || true"')
            print(f"  ✔ DataNode目录权限已恢复(700) on {node}: {dn_dir}")
        except:
            pass

    # 4. 验证恢复
    verify_recovery()

def verify_recovery():
    """验证故障已完全恢复"""
    print("\n▶ 验证恢复结果...")
    all_target_nodes = list(set([n for n, _ in _chaosblade_uids] + _fallback_nodes))
    all_clean = True
    for node in all_target_nodes:
        # 检查dd进程
        dd_check = run(f'ssh {node} "ps aux | grep -E \'(disk_stress|chaos_os|chaos_burnio)\' | grep -v grep || true"')
        if dd_check.strip():
            print(f"  ✘ {node}: 仍有残留进程: {dd_check.strip()}")
            all_clean = False
        else:
            print(f"  ✔ {node}: 无残留dd/chaos_os进程")

        # 检查临时文件
        tmp_check = run(f'ssh {node} "test -f /tmp/disk_stress && echo EXISTS || echo CLEAN"')
        if "EXISTS" in tmp_check:
            print(f"  ✘ {node}: /tmp/disk_stress 仍存在")
            all_clean = False
        else:
            print(f"  ✔ {node}: /tmp/disk_stress 已清理")

    # 检查目录权限
    for node, dn_dir in _dn_dirs_made_readonly:
        perm_check = run(f'ssh {node} "stat -c %a {dn_dir} 2>/dev/null || echo UNKNOWN"')
        if perm_check.strip() == "700":
            print(f"  ✔ {node}: {dn_dir} 权限正确(700)")
        else:
            print(f"  ⚠ {node}: {dn_dir} 权限为 {perm_check.strip()} (期望700)")

    if all_clean:
        print("  ✔ 所有节点恢复验证通过")
    else:
        print("  ⚠ 部分节点恢复可能不完整，请手动检查")

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
print("磁盘错误故障注入 (v3 - 健壮恢复版)")
print("=" * 60)

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/disk_error_output"

print(f"\n▶ 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  注入延迟: {INJECT_DELAY}s")
print(f"  目标节点: {TARGET_NODES}")

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

mark_fault_start("disk_error", {
    "duration": FAULT_DURATION,
    "target_nodes": TARGET_NODES,
    "inject_delay": INJECT_DELAY,
})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="disk_error_fault" \\
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

process = run_bg(cmd)

print(f"\n▶ 等待任务充分运行 ({INJECT_DELAY}s)...")
time.sleep(INJECT_DELAY)

print("\n▶ 查找运行中的应用...")
application_id = None
for attempt in range(30):
    try:
        result = run("yarn application -list 2>/dev/null")
        for line in result.split("\n"):
            if "disk_error_fault" in line and "RUNNING" in line:
                parts = line.split()
                for part in parts:
                    if part.startswith("application_"):
                        application_id = part
                        break
                if application_id:
                    break
    except:
        pass
    if application_id:
        break
    time.sleep(5)

if application_id:
    print(f"  ✔ 找到应用: {application_id}")
else:
    print("  ⚠ 未找到应用ID，继续执行故障注入")

# ===== 注入故障 =====
fallback_processes = []

for node in TARGET_NODES:
    print(f"\n▶ 注入磁盘故障 on {node}...")

    if check_chaosblade_available(node):
        # 方案1: ChaosBlade disk error (真正的磁盘I/O错误)
        print(f"  尝试ChaosBlade disk error...")
        try:
            result = run(f'ssh {node} "echo ubuntu | sudo -S {CHAOSBLADE_PATH} create disk error --read --write"')
            uid = None
            try:
                result_json = eval(result) if "{" in result else None
                if result_json and "uid" in result_json:
                    uid = result_json["uid"]
            except:
                uid_match = re.search(r'"uid"\s*:\s*"([^"]+)"', result)
                if uid_match:
                    uid = uid_match.group(1)

            if uid:
                _chaosblade_uids.append((node, uid))
                print(f"  ✔ ChaosBlade disk error UID: {uid}")
                mark_fault_injection("disk_error", node, "chaosblade_disk_error", FAULT_DURATION)
                continue
        except Exception as e:
            print(f"  ⚠ ChaosBlade disk error失败: {e}")

        # 方案1b: ChaosBlade disk burn (IO压力，作为备选)
        print(f"  尝试ChaosBlade disk burn (IO压力)...")
        try:
            result = run(f'ssh {node} "echo ubuntu | sudo -S {CHAOSBLADE_PATH} create disk burn --read --write"')
            uid = None
            try:
                result_json = eval(result) if "{" in result else None
                if result_json and "uid" in result_json:
                    uid = result_json["uid"]
            except:
                uid_match = re.search(r'"uid"\s*:\s*"([^"]+)"', result)
                if uid_match:
                    uid = uid_match.group(1)

            if uid:
                _chaosblade_uids.append((node, uid))
                print(f"  ✔ ChaosBlade disk burn UID: {uid}")
                mark_fault_injection("disk_error", node, "chaosblade_disk_burn", FAULT_DURATION)
                continue
        except Exception as e:
            print(f"  ⚠ ChaosBlade disk burn失败: {e}")

    # 方案2: 持续IO压力 + DataNode目录只读
    print(f"  使用降级方案: 持续IO压力 + DataNode目录只读")

    # 2a: 持续IO压力 (循环dd)
    try:
        dd_proc = subprocess.Popen(
            f'ssh {node} "echo ubuntu | sudo -S bash -c \'while true; do dd if=/dev/zero of=/tmp/disk_stress bs=1M count=4096 2>/dev/null; rm -f /tmp/disk_stress; done\'"',
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        fallback_processes.append((node, dd_proc))
        _fallback_nodes.append(node)
        print(f"  ✔ 持续IO压力已启动 on {node}")
    except Exception as e:
        print(f"  ⚠ IO压力启动失败: {e}")
        _fallback_nodes.append(node)

    # 2b: 设置DataNode数据目录只读
    try:
        dn_dir = '/opt/hadoop/data/datanode'
        if not dn_dir:
            dn_dir = "/opt/hadoop/data/datanode"

        run(f'ssh {node} "echo ubuntu | sudo -S chmod 444 {dn_dir} 2>/dev/null || true"')
        _dn_dirs_made_readonly.append((node, dn_dir))
        print(f"  ✔ DataNode目录设为只读: {dn_dir}")
    except Exception as e:
        print(f"  ⚠ DataNode目录只读设置失败: {e}")

    mark_fault_injection("disk_error", node, "fallback_io_pressure_readonly", FAULT_DURATION)

print(f"\n▶ 等待故障持续时间: {FAULT_DURATION}s...")
try:
    time.sleep(FAULT_DURATION)
except KeyboardInterrupt:
    print("\n  收到中断信号，提前终止...")

# ===== 恢复故障 =====
print(f"\n▶ 恢复故障...")

# 销毁ChaosBlade规则
for node, uid in _chaosblade_uids:
    try:
        result = run(f'ssh {node} "echo ubuntu | sudo -S {CHAOSBLADE_PATH} destroy {uid}"')
        print(f"  ✔ ChaosBlade规则已销毁 on {node}")
    except:
        print(f"  ⚠ ChaosBlade销毁失败 on {node}")

# 停止IO压力进程（先尝试terminate，再兜底killall）
for node, proc in fallback_processes:
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
    except:
        pass

# 兜底：在所有目标节点上杀掉所有dd和chaos_os进程
# CRITICAL: Must kill the while-true bash loop FIRST, then dd, otherwise dd gets restarted
for node in TARGET_NODES:
    try:
        # 1. Kill the while-true bash loop (parent of dd)
        run(f'ssh {node} "echo ubuntu | sudo -S pkill -9 -f \"while true.*disk_stress\" 2>/dev/null || true"')
        time.sleep(1)
        # 2. Kill remaining dd processes
        run(f'ssh {node} "echo ubuntu | sudo -S killall -9 dd 2>/dev/null || true"')
        # 3. Kill chaos_os
        run(f'ssh {node} "echo ubuntu | sudo -S killall -9 chaos_os 2>/dev/null || true"')
        # 4. Remove temp files
        run(f'ssh {node} "echo ubuntu | sudo -S rm -f /tmp/disk_stress 2>/dev/null || true"')
        # 5. Verify cleanup
        verify_result = run(f'ssh {node} "ps aux | grep -E \"disk_stress|chaos_os\" | grep -v grep | wc -l"')
        if verify_result.strip() != "0":
            # Force kill all related processes
            run(f'ssh {node} "echo ubuntu | sudo -S pkill -9 -f disk_stress 2>/dev/null || true"')
            run(f'ssh {node} "echo ubuntu | sudo -S pkill -9 -f chaos_os 2>/dev/null || true"')
            time.sleep(1)
            run(f'ssh {node} "echo ubuntu | sudo -S rm -f /tmp/disk_stress 2>/dev/null || true"')
        print(f"  ✔ 兜底进程清理和临时文件删除完成 on {node}")
    except:
        pass

# 恢复DataNode目录权限（使用700而非755）
for node, dn_dir in _dn_dirs_made_readonly:
    try:
        run(f'ssh {node} "echo ubuntu | sudo -S chmod 700 {dn_dir} 2>/dev/null || true"')
        print(f"  ✔ DataNode目录权限已恢复(700) on {node}: {dn_dir}")
    except:
        pass

# 标记清理完成，防止atexit重复执行
_cleanup_done = True

# 验证恢复
verify_recovery()

# 重启受影响的DataNode（disk_error降级方案会导致DataNode崩溃退出）
print("\n\u25b6 \u68c0\u67e5\u5e76\u91cd\u542f\u53d7\u5f71\u54cd\u7684DataNode...")
for node, dn_dir in _dn_dirs_made_readonly:
    try:
        # Check if DataNode is running
        dn_check = run(f'ssh {node} "jps | grep DataNode | wc -l"')
        if dn_check.strip() == "0":
            print(f"  \u26a0 DataNode on {node} \u672a\u8fd0\u884c\uff0c\u91cd\u542f\u4e2d...")
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
            time.sleep(2)
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            print(f"  \u2714 DataNode on {node} \u5df2\u91cd\u542f")
            time.sleep(10)
        else:
            print(f"  \u2714 DataNode on {node} \u6b63\u5e38\u8fd0\u884c")
    except Exception as e:
        print(f"  \u26a0 DataNode\u68c0\u67e5/\u91cd\u542f\u5931\u8d25 on {node}: {e}")
        # Fallback: try restart anyway
        try:
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
            time.sleep(2)
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            print(f"  \u2714 DataNode on {node} \u5df2\u91cd\u542f(\u5156\u5e95)")
            time.sleep(10)
        except Exception as e2:
            print(f"  \u2718 DataNode\u91cd\u542f\u5931\u8d25 on {node}: {e2}")

# Also check ChaosBlade-affected nodes
for node, uid in _chaosblade_uids:
    try:
        dn_check = run(f'ssh {node} "jps | grep DataNode | wc -l"')
        if dn_check.strip() == "0":
            print(f"  \u26a0 DataNode on {node} (ChaosBlade) \u672a\u8fd0\u884c\uff0c\u91cd\u542f\u4e2d...")
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true"')
            time.sleep(2)
            run(f'ssh {node} "/opt/hadoop/bin/hdfs --daemon start datanode"')
            print(f"  \u2714 DataNode on {node} \u5df2\u91cd\u542f")
            time.sleep(10)
    except Exception as e:
        print(f"  \u26a0 DataNode\u68c0\u67e5\u5931\u8d25 on {node}: {e}")

mark_fault_end("disk_error", {"result": "recovery_completed"})

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
    mark_fault_end("disk_error", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("disk_error", {"result": "completed_with_failures", "returncode": process.returncode})

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
print("磁盘错误故障注入完成 (v3)")
print("=" * 60)
