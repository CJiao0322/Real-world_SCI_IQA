import os

parent_dir = r"/Users/ttjiao/capture_all/4K_M"  # 改成你的父文件夹路径
suffix = "_M_dis_4k"

for name in os.listdir(parent_dir):
    old_path = os.path.join(parent_dir, name)
    if not os.path.isdir(old_path):
        continue

    if name.endswith(suffix):
        new_name = name[:-len(suffix)]
        new_path = os.path.join(parent_dir, new_name)

        if os.path.exists(new_path):
            print(f"跳过（已存在）: {new_name}")
            continue

        os.rename(old_path, new_path)
        print(f"{name}  ->  {new_name}")
