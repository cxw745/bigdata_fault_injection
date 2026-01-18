#!/usr/bin/env python3
import sys
for line in sys.stdin:
    text = line.strip()
    # 每行复制 4 份
    for i in range(4):
        print(f"{text}\t{i}")
