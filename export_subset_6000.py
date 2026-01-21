import os
import csv
import shutil
from pathlib import Path

# ====== 你只需要改这三个 ======
DATASET_ROOT = "/Users/ttjiao/capture_all"          # 原始数据根目录（30G那个）
MANIFEST_CSV = "manifest_6000.csv"                  # 你的 manifest
OUT_ROOT = "/Users/ttjiao/capture_subset_6000"     # 输出的新目录
# ============================

# 如果你的 manifest 不是 6000 行，而是 all，需要用一个筛选条件：
# 例如：只复制某个 manifest_6000.csv
# 或者在 manifest_all.csv 里有一列 "use" / "split" / "selected" 等
# 这里默认：manifest 里有哪些行就复制哪些行（你自己保证是 6000 行）

def human_size(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    x = float(n)
    for u in units:
        if x < 1024:
            return f"{x:.2f} {u}"
        x /= 1024
    return f"{x:.2f} PB"

def main():
    src_root = Path(DATASET_ROOT)
    out_root = Path(OUT_ROOT)
    out_root.mkdir(parents=True, exist_ok=True)

    if not Path(MANIFEST_CSV).exists():
        raise FileNotFoundError(f"找不到 manifest: {MANIFEST_CSV}")

    # 读取 rel_path
    rel_paths = []
    with open(MANIFEST_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "rel_path" not in reader.fieldnames:
            raise ValueError(f"manifest 缺少 rel_path 列，当前列：{reader.fieldnames}")
        for r in reader:
            rp = (r["rel_path"] or "").strip()
            if rp:
                rel_paths.append(rp)

    # 去重（避免重复拷贝）
    rel_paths = list(dict.fromkeys(rel_paths))
    print(f"✅ manifest 中待拷贝文件数（去重后）：{len(rel_paths)}")

    missing = []
    copied = 0
    total_bytes = 0

    for i, rp in enumerate(rel_paths, 1):
        src = src_root / rp
        dst = out_root / rp
        dst.parent.mkdir(parents=True, exist_ok=True)

        if not src.exists():
            missing.append(rp)
            continue

        # copy2 会保留时间戳等元信息（可选）
        shutil.copy2(src, dst)
        copied += 1
        try:
            total_bytes += src.stat().st_size
        except Exception:
            pass

        if i % 200 == 0 or i == len(rel_paths):
            print(f"Progress: {i}/{len(rel_paths)} | copied={copied} | missing={len(missing)}")

    print("\n====================")
    print(f"✅ copied:  {copied}")
    print(f"⚠️ missing: {len(missing)}")
    print(f"📦 copied size (sum of file sizes): {human_size(total_bytes)}")
    print(f"📁 output folder: {OUT_ROOT}")
    print("====================\n")

    if missing:
        miss_txt = out_root / "missing_files.txt"
        with open(miss_txt, "w", encoding="utf-8") as f:
            for rp in missing:
                f.write(rp + "\n")
        print(f"已写出缺失清单：{miss_txt}")

if __name__ == "__main__":
    main()
