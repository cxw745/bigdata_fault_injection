#!/usr/bin/env python3 -u
"""
网络延迟故障注入 (v3 - 健壮恢复版)

修复说明：
- v3修复：
  1. ChaosBlade destroy后始终额外执行tc qdisc del兜底清理
  2. 修复master节点命令执行逻辑：master节点(cxw-1)直接执行，不加ssh前缀
  3. 脚本开头和末尾都检查并清理残留tc规则
  4. 增加signal handler，确保Ctrl+C时也能清理
  5. 尝试保护SSH端口22的流量不受tc规则影响
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collect_data.unified_config import get_slave_nodes, get_master_node, DEFAULT_FAULT_PARAMS
from collect_data.fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

CHAOSBLADE_PATH = "/opt/chaosblade-1.7.2/blade"
FAULT_DURATION = int(os.environ.get("FAULT_DURATION", str(DEFAULT_FAULT_PARAMS.get("fault_duration", 120))))
LATENCY_MS = int(os.environ.get("LATENCY_MS", "2000"))
LOSS_PERCENT = int(os.environ.get("LOSS_PERCENT", "20"))
JITTER_MS = int(os.environ.get("JITTER_MS", "500"))
NETWORK_INTERFACE = "ens3"

# 全局状态，用于signal handler和atexit清理
_injected_nodes = []
_uid_map = {}
_tc_nodes = []
_cleanup_done = False

def run(cmd, check=True):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return e.stdout.strip() if e.stdout else "", e.stderr.strip() if e.stderr else ""

def run_bg(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def get_network_interface(node=None):
    if node:
        stdout, _ = run(f"ssh {node} \"ip route | head -1 | awk '{{print \\$5}}'\"", check=False)
        if stdout.strip():
            return stdout.strip()
    else:
        stdout, _ = run("ip route | head -1 | awk '{print $5}'", check=False)
        if stdout.strip():
            return stdout.strip()
    return NETWORK_INTERFACE

def is_local_node(node):
    """判断节点是否是本地节点（不需要SSH）"""
    master = get_master_node()
    return node == "localhost" or node == master

def build_remote_cmd(cmd, node):
    """根据节点构建命令，master/localhost直接执行，其他节点通过SSH"""
    if is_local_node(node):
        return cmd
    return f'ssh {node} "{cmd}"'

def cleanup_tc_on_node(node, interface):
    """在指定节点上清理tc规则（兜底清理）- 循环删除直到干净"""
    for _ in range(5):  # 最多尝试5次，处理嵌套qdisc
        cmd = f"echo ubuntu | sudo -S tc qdisc del dev {interface} root 2>/dev/null || true"
        full_cmd = build_remote_cmd(cmd, node)
        run(full_cmd, check=False)
        # 检查是否还有netem规则
        check_cmd = f"echo ubuntu | sudo -S tc qdisc show dev {interface} 2>/dev/null | grep netem || true"
        full_check = build_remote_cmd(check_cmd, node)
        stdout, _ = run(full_check, check=False)
        if not stdout.strip():
            break

def cleanup_residual_tc(nodes=None):
    """清理所有节点上的残留tc规则"""
    if nodes is None:
        nodes = get_slave_nodes() + [get_master_node()]
    print("\n▶ 检查并清理残留tc规则...")
    for node in nodes:
        interface = get_network_interface(node)
        check_cmd = f"echo ubuntu | sudo -S tc qdisc show dev {interface} 2>/dev/null | grep netem || true"
        full_check = build_remote_cmd(check_cmd, node)
        stdout, _ = run(full_check, check=False)
        if stdout.strip():
            print(f"  {node}: 发现残留tc规则，正在清理...")
            cleanup_tc_on_node(node, interface)
            stdout2, _ = run(full_check, check=False)
            if stdout2.strip():
                print(f"  {node}: ⚠ tc规则清理可能未成功: {stdout2.strip()}")
            else:
                print(f"  {node}: ✔ tc规则已清理")
        else:
            print(f"  {node}: 无残留tc规则")

def verify_recovery(nodes=None):
    """验证所有节点的tc规则已清除"""
    if nodes is None:
        nodes = get_slave_nodes() + [get_master_node()]
    print("\n▶ 验证恢复结果...")
    all_clean = True
    for node in nodes:
        interface = get_network_interface(node)
        check_cmd = f"echo ubuntu | sudo -S tc qdisc show dev {interface} 2>/dev/null | grep netem || true"
        full_check = build_remote_cmd(check_cmd, node)
        stdout, _ = run(full_check, check=False)
        if stdout.strip():
            print(f"  ✘ {node}: 仍有残留tc规则: {stdout.strip()}")
            all_clean = False
        else:
            print(f"  ✔ {node}: tc规则已完全清除")
    return all_clean

def do_cleanup():
    """执行完整的恢复清理逻辑"""
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True

    print("\n▶ 执行恢复清理...")
    all_nodes = _injected_nodes if _injected_nodes else (get_slave_nodes() + [get_master_node()])

    for node in all_nodes:
        interface = get_network_interface(node)
        # 1. 先尝试ChaosBlade destroy
        if node in _uid_map:
            for uid in _uid_map[node]:
                cmd = f"echo ubuntu | sudo -S {CHAOSBLADE_PATH} destroy {uid}"
                full_cmd = build_remote_cmd(cmd, node)
                run(full_cmd, check=False)
                print(f"  {node}: ChaosBlade规则 {uid} 已销毁")

        # 2. 始终额外执行tc qdisc del作为兜底清理
        cleanup_tc_on_node(node, interface)
        print(f"  {node}: tc兜底清理已执行")

    # 3. 最终验证
    verify_recovery(all_nodes)

def signal_handler(signum, frame):
    """处理中断信号"""
    sig_name = signal.Signals(signum).name
    print(f"\n⚠ 收到信号 {sig_name}，执行清理...")
    do_cleanup()
    sys.exit(130)

def inject_with_chaosblade(node, latency_ms, jitter_ms, loss_percent, interface):
    """使用ChaosBlade注入网络延迟+丢包"""
    print(f"  在 {node} 上注入网络延迟({latency_ms}ms) + 丢包({loss_percent}%)...")
    uids = []

    # 1. 注入延迟
    cmd = f"echo ubuntu | sudo -S {CHAOSBLADE_PATH} create network delay --time {latency_ms} --offset {jitter_ms} --interface {interface} --force"
    full_cmd = build_remote_cmd(cmd, node)

    stdout, stderr = run(full_cmd, check=False)
    uid = None
    if stdout:
        try:
            result = json.loads(stdout)
            if result.get("success"):
                uid = result.get("result", "").strip()
                print(f"    {node}: 延迟已注入 {latency_ms}ms, UID: {uid}")
        except json.JSONDecodeError:
            uid_match = re.search(r'"result"\s*:\s*"([^"]+)"', stdout)
            if uid_match:
                uid = uid_match.group(1)
                print(f"    {node}: 延迟已注入 {latency_ms}ms, UID: {uid}")
    if uid:
        uids.append(uid)

    # 2. 注入丢包
    if loss_percent > 0:
        cmd = f"echo ubuntu | sudo -S {CHAOSBLADE_PATH} create network loss --percent {loss_percent} --interface {interface} --force"
        full_cmd = build_remote_cmd(cmd, node)

        stdout, stderr = run(full_cmd, check=False)
        loss_uid = None
        if stdout:
            try:
                result = json.loads(stdout)
                if result.get("success"):
                    loss_uid = result.get("result", "").strip()
                    print(f"    {node}: 丢包已注入 {loss_percent}%, UID: {loss_uid}")
            except json.JSONDecodeError:
                uid_match = re.search(r'"result"\s*:\s*"([^"]+)"', stdout)
                if uid_match:
                    loss_uid = uid_match.group(1)
                    print(f"    {node}: 丢包已注入 {loss_percent}%, UID: {loss_uid}")
        if loss_uid:
            uids.append(loss_uid)

    return len(uids) > 0, uids

def inject_with_tc(node, latency_ms, jitter_ms, loss_percent, interface):
    """使用tc命令注入网络延迟+丢包（降级方案）"""
    print(f"  在 {node} 上使用tc注入网络延迟({latency_ms}ms) + 丢包({loss_percent}%)...")

    # 清理旧的tc规则
    cleanup_tc_on_node(node, interface)

    # 添加新的tc规则: netem delay + loss
    cmd = f"echo ubuntu | sudo -S tc qdisc add dev {interface} root netem delay {latency_ms}ms {jitter_ms}ms loss {loss_percent}%"
    full_cmd = build_remote_cmd(cmd, node)
    stdout, stderr = run(full_cmd, check=False)

    if stderr and "Cannot find" not in stderr:
        print(f"    {node}: tc注入可能有错误: {stderr}")
        return False

    print(f"    {node}: tc规则已添加")
    return True

def destroy_chaosblade(node, uids):
    for uid in uids:
        cmd = f"echo ubuntu | sudo -S {CHAOSBLADE_PATH} destroy {uid}"
        full_cmd = build_remote_cmd(cmd, node)
        run(full_cmd, check=False)
    print(f"  {node}: ChaosBlade规则已销毁")

def destroy_tc(node, interface):
    cleanup_tc_on_node(node, interface)
    print(f"  {node}: tc规则已删除")

def check_chaosblade_available(node=None):
    cmd = f"test -x {CHAOSBLADE_PATH} && echo 'ok'"
    full_cmd = build_remote_cmd(cmd, node) if node else cmd
    stdout, _ = run(full_cmd, check=False)
    return "ok" in stdout

def start_mapreduce_job():
    print("\n▶ 启动 MapReduce 任务...")
    hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
    hadoop_cmd = f"{hadoop_home}/bin/hadoop"
    input_path = "/HiBench/HiBench/Wordcount/Input"
    output_path = "/user/hadoop/network_latency_output"

    run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

    scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mapper_path = os.path.join(scripts_dir, "common_mapreduce", "mapper.py")
    reducer_path = os.path.join(scripts_dir, "common_mapreduce", "reducer.py")

    cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="network_latency_fault" \\
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
    print("  任务已启动")
    time.sleep(10)
    return process

def wait_for_running_job():
    print("\n▶ 等待任务进入RUNNING状态...")
    for i in range(30):
        try:
            list_output, _ = run("yarn application -list 2>/dev/null || true", check=False)
            running_lines = [
                line for line in list_output.splitlines()
                if "RUNNING" in line and line.strip().startswith("application_")
            ]
            if running_lines:
                app_id = running_lines[0].split()[0]
                print(f"  ✔ 任务正在运行: {app_id}")
                return app_id
        except:
            pass
        time.sleep(2)
    print("  ⚠ 未检测到RUNNING状态，继续执行故障注入...")
    return None

def main():
    global _injected_nodes, _uid_map, _tc_nodes

    # 注册signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(do_cleanup)

    print("=" * 60)
    print("网络延迟故障注入 (v3 - 健壮恢复版)")
    print("=" * 60)

    # ===== 脚本开头：检查并清理残留tc规则 =====
    slave_nodes = get_slave_nodes()
    master_node = get_master_node()
    all_nodes = slave_nodes + [master_node]
    cleanup_residual_tc(all_nodes)

    mr_process = start_mapreduce_job()
    app_id = wait_for_running_job()

    target_nodes = slave_nodes + [master_node]

    if len(sys.argv) > 1:
        specified_nodes = [n for n in sys.argv[1:] if n in slave_nodes or n == master_node]
        if specified_nodes:
            target_nodes = specified_nodes

    print(f"\n▶ 故障参数:")
    print(f"  目标节点: {', '.join(target_nodes)}")
    print(f"  延迟: {LATENCY_MS}ms (抖动: {JITTER_MS}ms)")
    print(f"  丢包率: {LOSS_PERCENT}%")
    print(f"  持续时间: {FAULT_DURATION}秒")

    print("\n▶ 检查注入工具可用性...")
    chaosblade_nodes = []
    tc_only_nodes = []
    for node in target_nodes:
        if check_chaosblade_available(node):
            chaosblade_nodes.append(node)
            print(f"   {node}: ChaosBlade可用")
        else:
            tc_only_nodes.append(node)
            print(f"   {node}: 仅tc可用")

    if not chaosblade_nodes and not tc_only_nodes:
        print(" ✘ 没有可用的节点，退出")
        try:
            mr_process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            mr_process.kill()
        sys.exit(1)

    print("\n▶ 开始注入网络延迟+丢包...")
    mark_fault_start("network_latency", {
        "target_nodes": target_nodes,
        "latency_ms": LATENCY_MS,
        "jitter_ms": JITTER_MS,
        "loss_percent": LOSS_PERCENT,
        "duration": FAULT_DURATION
    })

    # ChaosBlade节点
    for node in chaosblade_nodes:
        interface = get_network_interface(node)
        success, uids = inject_with_chaosblade(node, LATENCY_MS, JITTER_MS, LOSS_PERCENT, interface)
        if success:
            _injected_nodes.append(node)
            _uid_map[node] = uids
            mark_fault_injection("network_latency", node, "chaosblade_delay_loss", FAULT_DURATION)

    # tc降级节点
    for node in tc_only_nodes:
        interface = get_network_interface(node)
        success = inject_with_tc(node, LATENCY_MS, JITTER_MS, LOSS_PERCENT, interface)
        if success:
            _injected_nodes.append(node)
            _tc_nodes.append(node)
            mark_fault_injection("network_latency", node, "tc_delay_loss", FAULT_DURATION)

    if not _injected_nodes:
        print(" ✘ 所有节点注入失败")
        try:
            mr_process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            mr_process.kill()
        sys.exit(1)

    print(f"\n▶ 等待 {FAULT_DURATION} 秒...")
    try:
        time.sleep(FAULT_DURATION)
    except KeyboardInterrupt:
        print("\n  收到中断信号，提前终止...")

    # ===== 恢复故障 =====
    print("\n▶ 移除网络延迟+丢包...")
    for node in _injected_nodes:
        interface = get_network_interface(node)
        if node in _uid_map:
            destroy_chaosblade(node, _uid_map[node])
        if node in _tc_nodes:
            destroy_tc(node, interface)
        # 始终额外执行tc兜底清理
        cleanup_tc_on_node(node, interface)
        print(f"  {node}: tc兜底清理已执行")
        mark_fault_injection("network_latency", node, "remove_delay_loss", None)

    mark_fault_end("network_latency", {"injected_nodes": _injected_nodes})

    # ===== 脚本末尾：验证所有tc规则已清除 =====
    verify_recovery(_injected_nodes)

    # 清理完成，防止atexit重复执行
    global _cleanup_done
    _cleanup_done = True

    print("\n▶ 等待MapReduce任务完成...")
    try:
        mr_process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        mr_process.kill()
        mr_process.wait(timeout=5)

    print("\n" + "=" * 60)
    print("网络延迟故障注入完成 (v3)")
    print("=" * 60)

if __name__ == "__main__":
    main()
