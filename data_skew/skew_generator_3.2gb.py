#!/usr/bin/env python3
import os

output_dir = "data/skew_input"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "skew_3.2gb.txt")

# 目标大小 = 3.2GB = 3*1000^3 + 0.2*1000^3
TARGET_SIZE = int(3.2 * 1000 * 1000 * 1000)

HOT_KEY = "hot_key"
COLD_KEY = "cold_key"

hot_line = f"{HOT_KEY}\n"
cold_line = f"{COLD_KEY}\n"

hot_bytes = hot_line.encode("utf-8")
cold_bytes = cold_line.encode("utf-8")

hot_ratio = 0.99
cold_ratio = 1 - hot_ratio
    
print("▶ 使用 Python 快速生成 3.2GB 倾斜数据中...")

with open(output_file, "wb") as f:
    size = 0
    batch = bytearray()

    batch_limit = 5 * 1000 * 1000  # 每批写 5MB 提升速度

    while size < TARGET_SIZE:
        batch.clear()

        # 生成一批数据（4.95 MB 热点 + 0.05 MB 冷点）
        repeat_hot = int(batch_limit * hot_ratio / len(hot_bytes))
        repeat_cold = int(batch_limit * cold_ratio / len(cold_bytes))

        for _ in range(repeat_hot):
            batch.extend(hot_bytes)
        for _ in range(repeat_cold):
            batch.extend(cold_bytes)

        f.write(batch)
        size += len(batch)

        # 每写 200MB 输出一次进度
        if size % (200 * 1000 * 1000) < len(batch):
            print(f"  已生成: {size / (1000*1000*1000):.2f} GB")

print("✔ 完成 3.2GB 倾斜数据生成：", output_file)
os.system(f"ls -lh {output_file}")
