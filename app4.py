import streamlit as st
import os
import sqlite3
import csv
from datetime import datetime
from uuid import uuid4
from PIL import Image
import io
import base64
import streamlit.components.v1 as components
import random

st.set_page_config(layout="wide")

# =========================
# Config
# =========================
DB_PATH = "results.db"

# 你的“总数据父目录”，里面包含 4K/1080/4K_S/4K_M/1080_S/1080_M 等文件夹
DATASET_ROOT = "/Users/ttjiao/capture_all"

# 只包含 6000 行的 manifest（实验只用这 6000 张）
MANIFEST_CSV = "manifest_6000.csv"

TRAIN_DIR = "training_images"

LABELS = {1: "Bad", 2: "Poor", 3: "Fair", 4: "Good", 5: "Excellent"}
VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")

TRAIN_INTERVAL_MS = 3500

# 分配参数（你定的）
P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P   # 500
COVER_M = 2                                  # 每组合2张（覆盖包强度）

# =========================
# Database
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_ratings_columns(conn):
    """旧库迁移：补齐 text_clarity / color_correctness / image_id 三列"""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(ratings)")
    cols = {row[1] for row in cur.fetchall()}

    if "text_clarity" not in cols:
        cur.execute("ALTER TABLE ratings ADD COLUMN text_clarity TEXT")
    if "color_correctness" not in cols:
        cur.execute("ALTER TABLE ratings ADD COLUMN color_correctness TEXT")
    if "image_id" not in cols:
        cur.execute("ALTER TABLE ratings ADD COLUMN image_id TEXT")

    conn.commit()

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        student_id TEXT,
        device TEXT,
        screen_resolution TEXT,
        start_time TEXT
    )
    """)

    # 图片池（只导入 manifest_6000.csv 里那6000张）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS images (
        image_id TEXT PRIMARY KEY,
        rel_path TEXT NOT NULL,
        category INTEGER NOT NULL,
        category_name TEXT,
        resolution TEXT NOT NULL,
        distortion INTEGER NOT NULL,
        distortion_name TEXT,
        assigned_count INTEGER NOT NULL DEFAULT 0
    )
    """)

    # 分配表：每个参与者看到哪些图，顺序是什么
    cur.execute("""
    CREATE TABLE IF NOT EXISTS assignments (
        participant_id TEXT NOT NULL,
        image_id TEXT NOT NULL,
        ord INTEGER NOT NULL,
        assigned_time TEXT NOT NULL,
        PRIMARY KEY (participant_id, image_id)
    )
    """)

    # 评分表（支持你原来的三个问题）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        participant_id TEXT,
        image_name TEXT,
        score INTEGER,
        label TEXT,
        time TEXT,
        text_clarity TEXT,
        color_correctness TEXT,
        image_id TEXT
    )
    """)
    conn.commit()

    ensure_ratings_columns(conn)
    conn.close()

init_db()

def table_count(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])

def import_manifest_if_needed():
    if not os.path.exists(MANIFEST_CSV):
        st.error(f"找不到 {MANIFEST_CSV}，请确认它在程序目录中。")
        st.stop()

    conn = get_conn()
    try:
        if table_count(conn, "images") > 0:
            return

        cur = conn.cursor()
        with open(MANIFEST_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                image_id = r["image_id"].strip()
                rel_path = r["rel_path"].strip()

                category = int(r["category"])
                category_name = r.get("category_name", "").strip() or None

                resolution = r["resolution"].strip()
                distortion = int(r["distortion"])
                distortion_name = r.get("distortion_name", "").strip() or None

                rows.append((
                    image_id, rel_path, category, category_name,
                    resolution, distortion, distortion_name
                ))

        cur.executemany(
            """
            INSERT OR IGNORE INTO images
            (image_id, rel_path, category, category_name, resolution, distortion, distortion_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows
        )
        conn.commit()

        # 轻量校验
        n = table_count(conn, "images")
        if n != N_TARGET:
            st.warning(f"⚠️ images 表中共有 {n} 张，不等于期望 {N_TARGET}。若你确实只想用6000张，请确认 manifest_6000.csv 行数。")
    finally:
        conn.close()

import_manifest_if_needed()

# =========================
# Helpers
# =========================
@st.cache_data(show_spinner=False)
def list_images(folder):
    if not os.path.exists(folder):
        return []
    return sorted(
        f for f in os.listdir(folder)
        if f.lower().endswith(VALID_EXTS) and not f.startswith(".")
    )

@st.cache_data(show_spinner=False)
def image_as_data_url(img_path: str, max_side: int, quality: int = 92) -> str:
    with Image.open(img_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"

def get_assigned_image_ids(conn, pid: str):
    cur = conn.cursor()
    cur.execute("SELECT image_id FROM assignments WHERE participant_id=? ORDER BY ord ASC", (pid,))
    return [r[0] for r in cur.fetchall()]

def get_image_relpath(conn, image_id: str):
    cur = conn.cursor()
    cur.execute("SELECT rel_path FROM images WHERE image_id=?", (image_id,))
    r = cur.fetchone()
    return r[0] if r else None

def fetch_distinct_axes(conn):
    """从 images 表里动态取出：类别、分辨率、失真档（避免写死15类/R1R2）"""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT category FROM images ORDER BY category ASC")
    cats = [int(x[0]) for x in cur.fetchall()]

    cur.execute("SELECT DISTINCT resolution FROM images ORDER BY resolution ASC")
    ress = [x[0] for x in cur.fetchall()]

    cur.execute("SELECT DISTINCT distortion FROM images ORDER BY distortion ASC")
    dists = [int(x[0]) for x in cur.fetchall()]

    return cats, ress, dists

def build_strata_key(cat: int, res: str, dist: int) -> str:
    return f"{cat}|{res}|{dist}"

def fetch_available_by_strata(conn):
    """取出所有还能分配（assigned_count < R_TARGET）的图片，按 strata 分组，并按 assigned_count 少优先"""
    cur = conn.cursor()
    cur.execute("""
      SELECT image_id, category, resolution, distortion, assigned_count
      FROM images
      WHERE assigned_count < ?
    """, (R_TARGET,))
    strata = {}
    for image_id, cat, res, dist, assigned_count in cur.fetchall():
        key = build_strata_key(int(cat), str(res), int(dist))
        strata.setdefault(key, []).append((image_id, int(assigned_count)))

    for k in strata:
        strata[k].sort(key=lambda x: x[1])  # assigned_count 少的优先
    return strata

def assign_images_for_participant(pid: str):
    """
    给参与者 pid 分配 K_PER_PERSON=500 张图：
    - coverage pack：所有 (category × resolution × distortion) 每组合取 COVER_M 张
    - quota fill：补齐到500，优先选 assigned_count 少的
    并更新 images.assigned_count
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        existing = get_assigned_image_ids(conn, pid)
        if len(existing) >= K_PER_PERSON:
            return

        # 加锁：避免多人同时注册时抢 quota
        cur.execute("BEGIN IMMEDIATE")

        cats, ress, dists = fetch_distinct_axes(conn)
        strata = fetch_available_by_strata(conn)

        cover_ids = []
        for cat in cats:
            for res in ress:
                for dist in dists:
                    key = build_strata_key(cat, res, dist)
                    candidates = strata.get(key, [])
                    take = min(COVER_M, len(candidates))
                    for _ in range(take):
                        image_id, _ac = candidates.pop(0)
                        cover_ids.append(image_id)

        chosen = list(dict.fromkeys(cover_ids))  # 去重

        # 补齐到500
        if len(chosen) < K_PER_PERSON:
            pool = []
            for items in strata.values():
                for image_id, ac in items:
                    if image_id not in chosen:
                        pool.append((image_id, ac))
            pool.sort(key=lambda x: x[1])
            need = K_PER_PERSON - len(chosen)
            chosen.extend([x[0] for x in pool[:need]])

        if len(chosen) < K_PER_PERSON:
            st.warning(
                f"可分配图片不足：仅分到 {len(chosen)} 张（目标 {K_PER_PERSON}）。"
                f"可能是某些 strata 库存不足或配额已接近用尽。"
            )

        # 写 assignments + 更新 assigned_count
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        random.shuffle(chosen)

        cur.executemany(
            "INSERT OR IGNORE INTO assignments (participant_id, image_id, ord, assigned_time) VALUES (?, ?, ?, ?)",
            [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
        )

        cur.executemany(
            "UPDATE images SET assigned_count = assigned_count + 1 WHERE image_id = ?",
            [(img_id,) for img_id in chosen]
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# =========================
# Session State Init
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "idx" not in st.session_state:
    st.session_state.idx = 0

# =========================
# CSS
# =========================
st.markdown(
    """
    <style>
    .block-container{
        max-width: 100% !important;
        padding-left: 1.0rem;
        padding-right: 1.0rem;
    }
    div.stButton > button{
        width: 100%;
        padding: 1.05rem 1.2rem;
        border-radius: 16px;
        font-weight: 950;
        font-size: 18px;
        background-color: #111827;
        color: #ffffff;
        border: 2px solid #111827;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================
# Stage 0 — Intro
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    with st.form("intro_form"):
        student_id = st.text_input("Student ID / 学号", "")
        device = st.selectbox("Device / 设备", ["PC / Laptop", "Tablet", "Phone", "Other"])
        screen_resolution = st.selectbox(
            "Screen Resolution / 屏幕分辨率",
            ["Auto (recommended)", "1920x1080", "2560x1440", "3840x2160", "Other"]
        )
        submitted = st.form_submit_button("Start Experiment")

    if submitted:
        if student_id.strip() == "":
            st.error("Please enter your student ID.")
            return

        pid = str(uuid4())
        st.session_state.participant_id = pid

        conn = get_conn()
        conn.execute(
            "INSERT INTO participants VALUES (?, ?, ?, ?, ?)",
            (pid, student_id.strip(), device, screen_resolution, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()

        # 注册后立刻分配
        assign_images_for_participant(pid)

        st.session_state.stage = "training"
        st.rerun()

# =========================
# Stage 1 — Training
# =========================
def render_training():
    st.markdown(
        """
        <div style="text-align:center; font-weight:950; font-size:30px; margin-top:6px;">
          Training / 引导示例
        </div>
        <div style="text-align:center; opacity:0.7; font-weight:800; margin-top:6px; margin-bottom:10px;">
          请从图像清晰度，观感等方面，观察图像失真与对应的质量等级。质量越差，分数越低。完成培训后请下拉开始打分。
        </div>
        """,
        unsafe_allow_html=True,
    )

    train_imgs = list_images(TRAIN_DIR)
    if len(train_imgs) < 5:
        st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图。当前：{len(train_imgs)}")
        st.stop()

    train_imgs = train_imgs[:5]  # 前5张

    urls = [image_as_data_url(os.path.join(TRAIN_DIR, f), max_side=2400, quality=88) for f in train_imgs]
    caps = [f"{i+1} — {LABELS[i+1]}" for i in range(5)]

    components.html(
        f"""
        <div style="width:100%; display:flex; justify-content:center;">
          <div style="width:min(1800px, 98vw); text-align:center;">
            <img id="trainImg"
                 style="
                    width:100%;
                    height:auto;
                    max-height: 78vh;
                    object-fit: contain;
                    border-radius:18px;
                    border:1px solid rgba(0,0,0,0.10);
                    box-shadow:0 18px 48px rgba(0,0,0,0.18);
                    background:#fff;
                 " />
            <div id="trainCap"
                 style="margin-top:12px; font-size:24px; font-weight:950;"></div>
            <div style="opacity:0.65; font-weight:800; font-size:13px; margin-top:6px;">
              Training only · No scores recorded
            </div>
          </div>
        </div>

        <script>
          const urls = {urls};
          const caps = {caps};
          const interval = {TRAIN_INTERVAL_MS};

          const img = document.getElementById("trainImg");
          const cap = document.getElementById("trainCap");

          let i = 0;
          function show() {{
            img.src = urls[i];
            cap.textContent = caps[i];
          }}

          show();

          setTimeout(() => {{
            setInterval(() => {{
              i = (i + 1) % urls.length;
              show();
            }}, interval);
          }}, 700);
        </script>
        """,
        height=760,
    )

    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    if st.button("Next → Start Rating"):
        st.session_state.stage = "rating"
        st.session_state.idx = 0
        st.rerun()

# =========================
# Stage 2 — Rating (from assignments)
# =========================
def render_rating():
    pid = st.session_state.participant_id
    if not pid:
        st.error("No participant id.")
        st.stop()

    conn = get_conn()
    assigned_ids = get_assigned_image_ids(conn, pid)

    if len(assigned_ids) == 0:
        conn.close()
        st.error("该参与者没有分配到图片（assignments为空）。请检查 manifest 导入与分配流程。")
        st.stop()

    if st.session_state.idx >= len(assigned_ids):
        conn.close()
        st.session_state.stage = "done"
        st.rerun()
        return

    image_id = assigned_ids[st.session_state.idx]
    rel_path = get_image_relpath(conn, image_id)
    conn.close()

    if not rel_path:
        st.error("Image not found in DB (rel_path missing).")
        st.stop()

    img_path = os.path.join(DATASET_ROOT, rel_path)
    if not os.path.exists(img_path):
        st.error(f"找不到图片文件：{img_path}\n请检查 DATASET_ROOT 与 rel_path 是否匹配。")
        st.stop()

    left, right = st.columns([3.6, 1.4], gap="large")
    with left:
        st.image(img_path, caption=rel_path, use_container_width=True)

    with right:
        st.markdown("### Rate image quality")

        score = st.radio(
            "",
            options=[5, 4, 3, 2, 1],
            index=None,
            key=f"score_{st.session_state.idx}",
            format_func=lambda x: f"{x} — {LABELS[x]}",
            label_visibility="collapsed",
        )

        st.markdown("---")

        st.markdown("**Text clarity / 文本是否清晰**")
        text_clarity = st.radio(
            "",
            options=["Not clear", "OK", "Clear"],
            index=None,
            key=f"text_{st.session_state.idx}",
            label_visibility="collapsed",
        )

        st.markdown("**Color correctness / 颜色是否正确**")
        color_correctness = st.radio(
            "",
            options=["Correct", "Incorrect"],
            index=None,
            key=f"color_{st.session_state.idx}",
            label_visibility="collapsed",
        )

        next_clicked = st.button(
            "Next",
            disabled=(score is None or text_clarity is None or color_correctness is None),
        )

    if next_clicked:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO ratings (participant_id, image_name, score, label, time, text_clarity, color_correctness, image_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pid,
                rel_path,  # image_name 这里存相对路径更稳
                int(score),
                LABELS[int(score)],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                str(text_clarity),
                str(color_correctness),
                image_id,
            )
        )
        conn.commit()
        conn.close()

        st.session_state.idx += 1
        st.rerun()

# =========================
# Stage 3 — Done
# =========================
def render_done():
    st.success("Thank you for participating! / 感谢参与！")
    st.write("You may now close this page.")

# =========================
# Router
# =========================
if st.session_state.stage == "intro":
    render_intro()
elif st.session_state.stage == "training":
    render_training()
elif st.session_state.stage == "rating":
    render_rating()
elif st.session_state.stage == "done":
    render_done()
