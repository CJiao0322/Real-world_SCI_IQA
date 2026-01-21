import os

root_dir = "/Users/ttjiao/Desktop/Capture/No_Dis/1080"

count = 0
for root, dirs, files in os.walk(root_dir):
    for f in files:
        if f.lower().endswith(".png"):
            count += 1

print("PNG 图像总数:", count)
