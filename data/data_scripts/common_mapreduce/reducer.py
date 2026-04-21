#!/usr/bin/env python3
import sys
import random

current_key = None
current_sum = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 1)
    key = parts[0]
    val = 1

    if current_key == key:
        current_sum += val
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_sum}", flush=True)
        current_key = key
        current_sum = val

if current_key is not None:
    print(f"{current_key}\t{current_sum}", flush=True)
