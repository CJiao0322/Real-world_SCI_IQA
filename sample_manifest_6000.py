import csv
import random
from collections import defaultdict
import argparse
import math

def read_manifest(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # 统一字段
            r["category"] = int(r["category"])
            r["distortion"] = int(r["distortion"])
            # resolution 统一成两类：1080 / 4K
            res = r["resolution"].strip()
            if res.lower() == "4k":
                r["resolution"] = "4K"
            elif res == "1080":
                r["resolution"] = "1080"
            else:
                # 如果还有别的写法，你可以在这里补映射
                r["resolution"] = res
            rows.append(r)
    return rows

def largest_remainder_allocate(total, weights):
    """
    给一组 weights 分配整数，总和为 total。
    返回 dict(key -> int)
    """
    keys = list(weights.keys())
    wsum = sum(weights[k] for k in keys)
    if wsum <= 0:
        return {k: 0 for k in keys}

    raw = {k: total * (weights[k] / wsum) for k in keys}
    base = {k: int(math.floor(raw[k])) for k in keys}
    remain = total - sum(base.values())

    # 按小数部分从大到小补
    frac = sorted(keys, key=lambda k: (raw[k] - base[k]), reverse=True)
    for i in range(remain):
        base[frac[i % len(frac)]] += 1
    return base

def main(manifest_all, out_csv, total=6000, ratio="4:1", seed=42):
    random.seed(seed)

    rows = read_manifest(manifest_all)

    # 只保留 1080 和 4K
    rows = [r for r in rows if r["resolution"] in ("1080", "4K")]
    if len(rows) < total:
        raise RuntimeError(f"总图片不足：只有 {len(rows)} 张，无法抽 {total} 张")

    # 解析比例
    a, b = ratio.split(":")
    a, b = int(a), int(b)  # a=1080权重, b=4K权重
    target_1080 = int(round(total * a / (a + b)))
    target_4k = total - target_1080

    # 分层键：按 (category_name, distortion_name) 做分层，
    # 在每个层内再按分辨率比例抽
    strata = defaultdict(lambda: {"1080": [], "4K": []})
    for r in rows:
        key = (r["category_name"], r["distortion_name"])
        strata[key][r["resolution"]].append(r)

    # 先决定每个 strata 应该抽多少张（不分分辨率），按 strata 的总量比例
    weights = {k: (len(v["1080"]) + len(v["4K"])) for k, v in strata.items()}
    strata_total_alloc = largest_remainder_allocate(total, weights)

    # 然后在每个 strata 内按 1080:4K 比例抽（同时受各层实际库存限制）
    picked = []
    used_1080 = 0
    used_4k = 0

    # 为了稳定性，遍历顺序固定
    for key in sorted(strata_total_alloc.keys()):
        need = strata_total_alloc[key]
        if need <= 0:
            continue

        pool_1080 = strata[key]["1080"]
        pool_4k = strata[key]["4K"]

        # 理想分配
        need_1080 = int(round(need * a / (a + b)))
        need_4k = need - need_1080

        # 受库存限制修正
        take_1080 = min(need_1080, len(pool_1080))
        take_4k = min(need_4k, len(pool_4k))

        # 如果某一边不够，用另一边补齐
        remaining = need - (take_1080 + take_4k)
        if remaining > 0:
            # 先从库存多的一边补
            extra_1080 = min(remaining, len(pool_1080) - take_1080)
            take_1080 += extra_1080
            remaining -= extra_1080

        if remaining > 0:
            extra_4k = min(remaining, len(pool_4k) - take_4k)
            take_4k += extra_4k
            remaining -= extra_4k

        random.shuffle(pool_1080)
        random.shuffle(pool_4k)
        picked.extend(pool_1080[:take_1080])
        picked.extend(pool_4k[:take_4k])
        used_1080 += take_1080
        used_4k += take_4k

    # picked 可能因为库存限制不够 total（通常不会），做一次全局补齐
    if len(picked) < total:
        remaining = total - len(picked)
        # 先按全局目标优先补到目标比例
        already_ids = set(r["image_id"] for r in picked)
        pool_1080_all = [r for r in rows if r["resolution"] == "1080" and r["image_id"] not in already_ids]
        pool_4k_all = [r for r in rows if r["resolution"] == "4K" and r["image_id"] not in already_ids]
        random.shuffle(pool_1080_all)
        random.shuffle(pool_4k_all)

        # 先补缺的那边
        need_1080_more = max(0, target_1080 - used_1080)
        take_1080 = min(need_1080_more, remaining, len(pool_1080_all))
        picked.extend(pool_1080_all[:take_1080])
        used_1080 += take_1080
        remaining -= take_1080

        if remaining > 0:
            need_4k_more = max(0, target_4k - used_4k)
            take_4k = min(need_4k_more, remaining, len(pool_4k_all))
            picked.extend(pool_4k_all[:take_4k])
            used_4k += take_4k
            remaining -= take_4k

        # 还剩就随便补
        if remaining > 0:
            pool_any = pool_1080_all[take_1080:] + pool_4k_all[min(need_4k_more, len(pool_4k_all)):]
            random.shuffle(pool_any)
            picked.extend(pool_any[:remaining])

    # 多了就截断
    picked = picked[:total]

    # 最终统计
    final_1080 = sum(1 for r in picked if r["resolution"] == "1080")
    final_4k = sum(1 for r in picked if r["resolution"] == "4K")

    # 写出
    fieldnames = ["image_id", "rel_path", "category", "category_name", "resolution", "distortion", "distortion_name"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(picked)

    print(f"✅ wrote {len(picked)} rows to {out_csv}")
    print(f"✅ resolution counts: 1080={final_1080}, 4K={final_4k} (target 1080={target_1080}, 4K={target_4k})")
    print(f"✅ seed={seed}, ratio={ratio}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="manifest_all.csv")
    ap.add_argument("--out", required=True, help="manifest_6000.csv")
    ap.add_argument("--ratio", default="4:1", help="1080:4K 比例，如 4:1 或 3:2")
    ap.add_argument("--total", type=int, default=6000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    main(args.inp, args.out, total=args.total, ratio=args.ratio, seed=args.seed)
