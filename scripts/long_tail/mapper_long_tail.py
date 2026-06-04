#!/usr/bin/env python3
import sys, os, time, random, signal

HANG_MODE = "sleep"      # 挂起模式："none" | "sleep" | "busy" | "stop"
HANG_PROB = 0.3          # 挂起概率
HANG_SECONDS = 120      # 挂起秒数

def maybe_inject_hang():
    if HANG_MODE == "none": return
    if random.random() >= HANG_PROB: return

    pid = os.getpid() 
    # sys.stderr.write(f"[inject] mode={HANG_MODE} pid={pid} seconds={HANG_SECONDS}\n")
    # sys.stderr.flush()

    # 简单挂起（进程可自行恢复）
    if HANG_MODE == "sleep":
        time.sleep(HANG_SECONDS)
        return

    # 忙等
    if HANG_MODE == "busy":
        end = time.time() + HANG_SECONDS
        while time.time() < end:
            x = 0
            for i in range(100000):
                x += i*i
        return
    
    # 冻结（需要外部唤醒）
    if HANG_MODE == "stop":
        # 发 SIGSTOP 给自己，需外部发 SIGCONT 恢复
        os.kill(pid, signal.SIGSTOP)
        return

maybe_inject_hang()

for line in sys.stdin:
    line = line.strip()
    if not line: 
        continue
    for word in line.split():
        print(f"{word}\t1", flush=True)
