#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line: 
        continue
    for word in line.split():
        print(f"{word}\t1", flush=True)
