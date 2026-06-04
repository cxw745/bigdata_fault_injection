#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    key, val = line.strip().split("\t")
    val = int(val)
    if current_key == key:
        current_sum += val
    else:
        if current_key:
            print(f"{current_key}\t{current_sum}")
        current_key = key
        current_sum = val

if current_key:
    print(f"{current_key}\t{current_sum}")
