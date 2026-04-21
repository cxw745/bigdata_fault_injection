#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if line.startswith("hot_key"):
        print(f"hot_key\t1")
    else:
        print(f"cold_key\t1")
