#!/usr/bin/env python3
import sys
import random

CRASH_PROB = 0.2  # 10% 概率发生 crash

# 随机 crash
if random.random() < CRASH_PROB:
    # sys.stderr.write("mapper injected crash\n")
    # sys.stderr.flush()
    raise RuntimeError("mapper injected crash")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    for word in line.split():
        print(f"{word}\t1", flush=True)
        