POSTGRES_DSN = "postgresql://postgres.sswmzxkgdmzuelhpmvee:[skype%408790%21]@aws-1-ap-south-1.pooler.supabase.com:5432/postgres?sslmode=require"

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
import time

from streamlit_js_eval import streamlit_js_eval

st.set_page_config(layout="wide")

# =========================
# Config
# =========================
DB_PATH = "results.db"

DATASET_ROOT = "/Users/ttjiao/capture_all"
MANIFEST_CSV = "manifest_6000.csv"
TRAIN_DIR = "training_images"

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然",
}

VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")

# 训练轮播速度（你希望更慢）
TRAIN_INTERVAL_MS = 7000

# 分配参数
P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P   # 500
COVER_M = 2                                  # 每组合2张（覆盖包强度）

# SQLite 写入重试参数
SQLITE_TIMEOUT_SEC = 30
SQLITE_BUSY_TIMEOUT_MS = 8000
SQLITE_WRITE_RETRIES = 6


# =========================
# Database utilities
# =========================
def get_conn():
    """
    关键：timeout + WAL + busy_timeout
    WAL 能显著降低“database is locked”
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=SQLITE_TIMEOUT_SEC)
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


def execute_write_with_retry(conn: sqlite3.Connection, sql: str, params=None):
    """所有写入走这个：遇到 locked 就指数退避重试"""
    if params is None:
        params = ()
    for i in range(SQLITE_WRITE_RETRIES):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg:
                time.sleep(0.15 * (2 ** i))
                continue
            raise
    raise sqlite3.OperationalError("database is locked (exceeded retries)")


def executemany_write_with_retry(conn: sqlite3.Connection, sql: str, seq_params):
    for i in range(SQLITE_WRITE_RETRIES):
        try:
            conn.executemany(sql, seq_params)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg:
                time.sleep(0.15 * (2 ** i))
                continue
            raise
    raise sqlite3.OperationalError("database is locked (exceeded retries)")


def ensure_ratings_columns(conn):
    """
    旧库迁移：补齐 ratings 必须字段
    注意：我们现在不再使用 color_correctness
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(ratings)")
    cols = {row[1] for row in cur.fetchall()}

    if "text_clarity" not in cols:
        execute_write_with_retry(conn, "ALTER TABLE ratings ADD COLUMN text_clarity TEXT")
    if "image_id" not in cols:
        execute_write_with_retry(conn, "ALTER TABLE ratings ADD COLUMN image_id TEXT")

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS assignments (
        participant_id TEXT NOT NULL,
        image_id TEXT NOT NULL,
        ord INTEGER NOT NULL,
        assigned_time TEXT NOT NULL,
        PRIMARY KEY (participant_id, image_id)
    )
    """)

    # ✅ ratings：彻底移除 color_correctness
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        participant_id TEXT,
        image_name TEXT,
        score INTEGER,
        label TEXT,
        time TEXT,
        text_clarity TEXT,
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

        executemany_write_with_retry(
            conn,
            """
            INSERT OR IGNORE INTO images
            (image_id, rel_path, category, category_name, resolution, distortion, distortion_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows
        )
        conn.commit()

        n = table_count(conn, "images")
        if n != N_TARGET:
            st.warning(
                f"⚠️ images 表中共有 {n} 张，不等于期望 {N_TARGET}。"
                f"若你确实只想用6000张，请确认 {MANIFEST_CSV} 行数。"
            )
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
    """Training 页：编码成 data URL 做轮播（避免 rerun），这里会压缩成 JPEG"""
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
        strata[k].sort(key=lambda x: x[1])
    return strata


def assign_images_for_participant(pid: str):
    """
    给参与者 pid 分配 K_PER_PERSON 张：
    - coverage：每个 (cat×res×dist) 取 COVER_M
    - fill：补齐到 K_PER_PERSON，优先 assigned_count 少
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        existing = get_assigned_image_ids(conn, pid)
        if len(existing) >= K_PER_PERSON:
            return

        # BEGIN IMMEDIATE + 重试
        for i in range(SQLITE_WRITE_RETRIES):
            try:
                cur.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    time.sleep(0.15 * (2 ** i))
                    continue
                raise

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

        chosen = list(dict.fromkeys(cover_ids))

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

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        random.shuffle(chosen)

        executemany_write_with_retry(
            conn,
            "INSERT OR IGNORE INTO assignments (participant_id, image_id, ord, assigned_time) VALUES (?, ?, ?, ?)",
            [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
        )

        executemany_write_with_retry(
            conn,
            "UPDATE images SET assigned_count = assigned_count + 1 WHERE image_id = ?",
            [(img_id,) for img_id in chosen]
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
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
# Stage 0 — Intro
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    with st.form("intro_form"):
        student_id = st.text_input("Student ID / 学号", "")
        device = st.selectbox("Device / 设备", ["PC / Laptop", "Tablet", "Phone", "Other"])

        # ✅ 自动检测：物理分辨率（仅当用户选择“我不知道(自动检测)”时用）
        detected_physical = streamlit_js_eval(
            js_expressions="""
            (() => {
              const sw = screen.width, sh = screen.height;
              const dpr = window.devicePixelRatio || 1;
              const pw = Math.round(sw * dpr);
              const ph = Math.round(sh * dpr);
              return `${pw}x${ph}`;
            })()
            """,
            key="DETECTED_PHYSICAL",
            want_output=True,
        )

        # ✅ 手动优先 + 我不知道(自动检测)
        resolution_choice = st.selectbox(
            "Screen Resolution / 请选择屏幕分辨率（优先选择你显示器的物理分辨率）",
            [
                "3840×2160",
                "2560×1440",
                "1920×1080",
                "I don’t know (auto-detect) / 我不知道（自动检测）",
                "Other / 其他",
            ],
        )

        if resolution_choice.startswith("I don’t know"):
            st.caption(f"Auto-detected physical resolution: {detected_physical}")

        submitted = st.form_submit_button("Start Experiment")

    if not submitted:
        return

    if student_id.strip() == "":
        st.error("Please enter your student ID.")
        return

    # ✅ 最终写入数据库的 screen_resolution（只存物理分辨率）
    if resolution_choice.startswith("I don’t know"):
        if detected_physical:
            screen_resolution = f"auto:{detected_physical}"
        else:
            screen_resolution = "auto:unknown"
    elif resolution_choice.startswith("Other"):
        screen_resolution = "manual:other"
    else:
        screen_resolution = f"manual:{resolution_choice.replace('×', 'x')}"

    pid = str(uuid4())
    st.session_state.participant_id = pid

    conn = get_conn()
    try:
        execute_write_with_retry(
            conn,
            "INSERT INTO participants VALUES (?, ?, ?, ?, ?)",
            (pid, student_id.strip(), device, screen_resolution, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
    finally:
        conn.close()

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
        <div style="text-align:center; opacity:0.85; font-weight:800; margin-top:10px; margin-bottom:12px; line-height:1.7;">
          请观察图像整体质量，并理解不同评分等级的含义。<br/>
          评分主要依据：清晰度、自然程度、以及是否存在明显失真（模糊、噪声、伪影、压缩痕迹、文本难以辨认等）。<br/>
          <b>Bad / Poor</b>：失真明显，影响观看体验；<br/>
          <b>Fair</b>：存在一定失真，但仍可接受；<br/>
          <b>Good / Excellent</b>：图像清晰自然，几乎无明显失真。<br/>
          以下示例仅用于帮助理解评分标准，不会记录分数。培训完请点击下方按钮开始打分。
        </div>
        """,
        unsafe_allow_html=True,
    )

    train_imgs = list_images(TRAIN_DIR)
    if len(train_imgs) < 5:
        st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图。当前：{len(train_imgs)}")
        st.stop()

    train_imgs = train_imgs[:5]
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
                 style="margin-top:12px; font-size:22px; font-weight:950;"></div>
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
        st.session_state.rating_start_ts = time.time()
        st.rerun()


# =========================
# Stage 2 — Rating
# =========================
def render_rating():
    pid = st.session_state.participant_id
    if not pid:
        st.error("No participant id.")
        st.stop()

    conn = get_conn()
    assigned_ids = get_assigned_image_ids(conn, pid)

    total = len(assigned_ids)
    done = st.session_state.idx

    if "rating_start_ts" not in st.session_state:
        st.session_state.rating_start_ts = time.time()

    elapsed = time.time() - st.session_state.rating_start_ts
    done_for_avg = max(1, done)
    sec_per = elapsed / done_for_avg
    remaining_sec = max(0, (total - done) * sec_per)

    st.progress(done / total if total else 0, text=f"Progress: {done}/{total} images completed")
    st.caption(f"Elapsed: {elapsed/60:.1f} min · Avg: {sec_per:.1f}s/image · ETA: {remaining_sec/60:.1f} min")

    if total == 0:
        conn.close()
        st.error("该参与者没有分配到图片（assignments为空）。请检查 manifest 导入与分配流程。")
        st.stop()

    if st.session_state.idx >= total:
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
        # ✅ 评分页显示原始图
        st.image(img_path, caption=rel_path, use_container_width=True)

    with right:
        st.markdown("### Rate image quality / 图像质量评分")

        score = st.radio(
            "",
            options=[5, 4, 3, 2, 1],
            index=None,
            key=f"score_{st.session_state.idx}",
            format_func=lambda x: f"{x} — {LABELS[x]}",
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("**Text clarity / 文本清晰度**")
        text_clarity = st.radio(
            "",
            options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
            index=None,
            key=f"text_{st.session_state.idx}",
            label_visibility="collapsed",
        )

        next_clicked = st.button(
            "Next",
            disabled=(score is None or text_clarity is None),
        )

    if next_clicked:
        conn = get_conn()
        try:
            execute_write_with_retry(
                conn,
                """
                INSERT INTO ratings (participant_id, image_name, score, label, time, text_clarity, image_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    rel_path,  # image_name 用 rel_path 更稳
                    int(score),
                    LABELS[int(score)],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    str(text_clarity),
                    image_id,
                )
            )
            conn.commit()
        finally:
            conn.close()

        st.session_state.idx += 1
        st.rerun()


# =========================
# Stage 3 — Done
# =========================
def render_done():
    if "rating_start_ts" in st.session_state:
        del st.session_state["rating_start_ts"]

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
