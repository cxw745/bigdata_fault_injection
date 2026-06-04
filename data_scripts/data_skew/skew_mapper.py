#!/usr/bin/env python3
import sys
import os
import random

SKEW_RATIO = float(os.environ.get("SKEW_RATIO", "0.8"))

for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        if random.random() < SKEW_RATIO:
            print(f"SKEW_KEY\t1")
        else:
            print(f"{word}\t1")