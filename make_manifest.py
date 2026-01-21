import os
import csv
import argparse
from collections import defaultdict

VALID_EXTS = (".png",)

def parse_outer_folder(name: str):
    n = name.strip()
    n_low = n.lower()

    # 分辨率判断（注意这里必须用小写）
    if n_low.startswith("4k"):
        res = "4K"
    elif n_low.startswith("1080"):
        res = "1080"
    else:
        return None

    # 失真档判断（这里没问题）
    if "_s" in n_low:
        dist = 3
        dist_name = "S"
    elif "_m" in n_low:
        dist = 2
        dist_name = "M"
    else:
        dist = 1
        dist_name = "base"

    return res, dist, dist_name


def main(root, out_csv):
    # 扫描：root / outer(6个) / class(10~15个) / *.png
    rows = []
    all_classes = set()
    stats = defaultdict(int)

    if not os.path.isdir(root):
        raise RuntimeError(f"Root folder not found: {root}")

    outers = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    if not outers:
        raise RuntimeError(f"No subfolders found under: {root}")

    for outer in sorted(outers):
        parsed = parse_outer_folder(outer)
        if not parsed:
            # 跳过不认识的文件夹（比如 .DS_Store 或其他）
            continue
        res, dist, dist_name = parsed

        outer_path = os.path.join(root, outer)
        class_dirs = [c for c in os.listdir(outer_path) if os.path.isdir(os.path.join(outer_path, c))]

        for cls in sorted(class_dirs):
            cls_path = os.path.join(outer_path, cls)
            all_classes.add(cls)

            for fn in sorted(os.listdir(cls_path)):
                if not fn.lower().endswith(VALID_EXTS):
                    continue
                rel_path = os.path.join(outer, cls, fn).replace("\\", "/")
                image_id = rel_path  # 用相对路径做唯一ID，避免 0001.png 冲突

                rows.append({
                    "image_id": image_id,
                    "rel_path": rel_path,
                    "category_name": cls,
                    "resolution": res,
                    "distortion": dist,
                    "distortion_name": dist_name,
                })
                stats[(res, dist_name, cls)] += 1

    if not rows:
        raise RuntimeError("No PNG images found. Please check folder structure and extensions.")

    # 给 category 编号（按字母排序稳定映射 1..N）
    class_list = sorted(all_classes)
    class_to_id = {c: i + 1 for i, c in enumerate(class_list)}
    for r in rows:
        r["category"] = class_to_id[r["category_name"]]

    # 写出 CSV
    fieldnames = ["image_id", "rel_path", "category", "category_name", "resolution", "distortion", "distortion_name"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # 打印简单统计，帮助你检查每个组合是否都有图
    print(f"✅ Wrote {len(rows)} rows to {out_csv}")
    print(f"✅ Found {len(class_list)} classes: {class_list[:5]} ...")
    # 统计每个 (resolution, distortion_name) 总数
    agg = defaultdict(int)
    for r in rows:
        agg[(r["resolution"], r["distortion_name"])] += 1
    print("Counts by (resolution, distortion):")
    for k in sorted(agg):
        print(f"  {k}: {agg[k]}")

    # 可选：检查是否有“某类在某档下为0张”
    # 只检查这6个桶
    expected_buckets = [("4K","base"),("4K","S"),("4K","M"),("1080","base"),("1080","S"),("1080","M")]
    missing = []
    for cls in class_list:
        for res, dname in expected_buckets:
            # 是否出现过
            found = any((rr["category_name"] == cls and rr["resolution"] == res and rr["distortion_name"] == dname) for rr in rows)
            if not found:
                missing.append((cls, res, dname))
    if missing:
        print(f"⚠️ Warning: {len(missing)} missing (class, resolution, distortion) combos (0 images). Example:")
        print("  ", missing[:10])
    else:
        print("✅ All classes appear in all 6 (resolution, distortion) buckets.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="父目录，里面包含 4K / 1080 / 4K_S / 4K_M / 1080_S / 1080_M 等文件夹")
    ap.add_argument("--out", default="manifest_all.csv", help="输出 manifest.csv 路径")
    args = ap.parse_args()
    main(args.root, args.out)
