#!/usr/bin/env python3
"""
数据膨胀故障注入 - Mapper

在Mapper代码中将输入数据膨胀输出，产生大量中间数据，导致：
1. Map阶段输出数据量急剧增加
2. Shuffle阶段网络传输压力增大
3. 磁盘IO压力增大

参数：
- BLOAT_FACTOR: 每个输入记录膨胀输出的次数（默认20倍）
"""
import sys
import os

BLOAT_FACTOR = int(os.environ.get("BLOAT_FACTOR", "20"))

for line in sys.stdin:
    text = line.strip()
    if not text:
        continue
    words = text.split()
    for word in words:
        for _ in range(BLOAT_FACTOR):
            print(f"{word}\t1", flush=True)
