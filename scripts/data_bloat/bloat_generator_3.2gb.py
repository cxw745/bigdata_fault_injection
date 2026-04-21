#!/usr/bin/env python3
import os

output_dir = "data/bloat_input"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "bloat_3.2gb.txt")

# 目标大小 = 3.2GB
TARGET_SIZE = int(3.2 * 1000 * 1000 * 1000)

# 一个较长的基础行（数据膨胀场景）
base_line = (
    "VeryLargeExpandedValueABCDEFGHIJKLMNOPQRSTUVWXZY1234567890"
    "0987654321ZYXWVUTSRQPONMLKJIHGFEDCBA\n"
)

base_bytes = base_line.encode("utf-8")

print("▶ 使用 Python 快速生成 3.2GB 膨胀数据中...")

with open(output_file, "wb") as f:
    size = 0
    batch = bytearray()
    counter = 0  # 用于生成不同 key

    batch_limit = 5 * 1000 * 1000  # 每批写入 5MB

    while size < TARGET_SIZE:
        batch.clear()
        repeat_count = batch_limit // len(base_bytes)

        for _ in range(repeat_count):
            # 每行加上唯一 id，保证 key 不一样
            line = f"bloat_key_{counter}\t{base_line}".encode("utf-8")
            batch.extend(line)
            counter += 1

        f.write(batch)
        size += len(batch)

        if size % (200 * 1000 * 1000) < len(batch):
            print(f"  已生成: {size / (1000*1000*1000):.2f} GB")

print("✔ 完成 3.2GB 膨胀数据生成：", output_file)
os.system(f"ls -lh {output_file}")
