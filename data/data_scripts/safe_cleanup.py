#!/usr/bin/env python3
"""
安全清理脚本 - 只删除指定application_id的样本目录
绝不删除整个batch目录！绝不删除fault_labels.csv！
"""
import os, csv, shutil, sys
from collections import defaultdict

def safe_remove_incomplete_samples(data_dir, dry_run=True):
    """只删除metrics缺失的样本目录，绝不删除batch目录"""
    total_removed = 0
    removed_by_type = defaultdict(int)

    for batch in sorted(os.listdir(data_dir)):
        bp = os.path.join(data_dir, batch)
        if not os.path.isdir(bp) or not batch.startswith('batch_'):
            continue

        csv_path = os.path.join(bp, 'fault_labels.csv')
        if not os.path.exists(csv_path):
            print(f'  跳过 {batch}: 无fault_labels.csv')
            continue

        # 读取所有记录
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))

        if not rows:
            print(f'  跳过 {batch}: fault_labels.csv为空（不删除batch目录！）')
            continue

        valid_rows = []
        for row in rows:
            app_id = row.get('application_id', '')
            ft = row.get('fault_type', '')
            if not app_id or ft == 'fault_type':
                continue

            metrics_dir = os.path.join(bp, app_id, 'metrics')
            if os.path.exists(metrics_dir) and os.listdir(metrics_dir):
                valid_rows.append(row)
            else:
                # 只删除该样本目录
                app_dir = os.path.join(bp, app_id)
                if os.path.exists(app_dir):
                    if dry_run:
                        print(f'  [DRY-RUN] 将删除: {app_id} (type={ft})')
                    else:
                        shutil.rmtree(app_dir)
                    total_removed += 1
                    removed_by_type[ft] += 1

        # 重写fault_labels.csv - 但绝不删除batch目录！
        if valid_rows and total_removed > 0:
            if dry_run:
                print(f'  [DRY-RUN] 将更新 {batch}/fault_labels.csv: {len(valid_rows)}/{len(rows)}条保留')
            else:
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=valid_rows[0].keys())
                    writer.writeheader()
                    writer.writerows(valid_rows)
        elif not valid_rows:
            # 即使所有样本都无效，也不删除batch目录！
            print(f'  ⚠️ {batch}: 所有样本无效，但不会删除batch目录！请手动处理')

    return total_removed, removed_by_type

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='安全清理不完整样本')
    parser.add_argument('--data-dir', required=True, help='数据目录')
    parser.add_argument('--execute', action='store_true', help='实际执行（默认dry-run）')
    args = parser.parse_args()

    mode = '执行' if args.execute else 'DRY-RUN（预览）'
    print(f'模式: {mode}')
    print(f'目录: {args.data_dir}')
    print()

    total, by_type = safe_remove_incomplete_samples(args.data_dir, dry_run=not args.execute)

    print()
    print(f'总计: {total}条样本将被删除')
    for ft, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f'  {ft}: {cnt}')

    if not args.execute:
        print()
        print('这是预览模式。添加 --execute 参数才会实际执行删除。')
