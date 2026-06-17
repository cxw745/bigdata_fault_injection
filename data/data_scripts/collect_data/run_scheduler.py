#!/usr/bin/env python3
"""Wrapper script to run unified_scheduler_v2.py with sequence from file"""

import argparse
import subprocess
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEDULER = os.path.join(SCRIPT_DIR, "unified_scheduler_v2.py")

def main():
    parser = argparse.ArgumentParser(description="Run scheduler with sequence from file")
    parser.add_argument("--sequence-file", required=True, help="Path to file containing sequence string")
    parser.add_argument("--data-size", default="medium")
    parser.add_argument("--workload", default="wordcount")
    parser.add_argument("--interval-min", type=int, default=60)
    parser.add_argument("--interval-max", type=int, default=400)
    parser.add_argument("--max-batch-size", type=int, default=100)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    with open(args.sequence_file, "r") as f:
        sequence = f.read().strip()

    if not sequence:
        print("ERROR: Sequence file is empty!")
        sys.exit(1)

    cmd = [
        sys.executable, SCHEDULER,
        "--mode", "sequential",
        "--sequence", sequence,
        "--data-size", args.data_size,
        "--workload", args.workload,
        "--skip-prepare",
        "--interval-min", str(args.interval_min),
        "--interval-max", str(args.interval_max),
        "--max-batch-size", str(args.max_batch_size),
    ]

    if args.output_dir:
        cmd.extend(["--output-dir", args.output_dir])

    print(f"Starting scheduler with {len(sequence.split(','))} tasks...")
    print(f"Workload: {args.workload}, Data size: {args.data_size}")
    print(f"Command: {' '.join(cmd[:8])} ... (sequence omitted)")

    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
