cd /Users/ttjiao/Desktop/Est_data

git init
git add .
git commit -m "initial commit"

git branch -M main

# ⚠️ 关键改动在这里
git remote set-url origin https://github.com/CJiao0322/Real-world_SCI_IQA.git

git push -u origin main
