import os
import csv
import time
import random
from datetime import datetime
from uuid import uuid4

import streamlit as st
import psycopg
from psycopg_pool import ConnectionPool
from streamlit_js_eval import streamlit_js_eval

st.set_page_config(layout="wide")

# =========================
# Config
# =========================
DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://USER:PASSWORD@HOST:6543/postgres?sslmode=require",
)

R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)

MANIFEST_CSV = "manifest_6000.csv"
TRAIN_DIR = "training_images"

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然",
}

TRAIN_INTERVAL_MS = 7000

# 分配参数
P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P  # 500
COVER_M = 2

# =========================
# PG Pool (IMPORTANT!)
# =========================
@st.cache_resource
def get_pool():
    # 关键：prepare_threshold=0 -> 禁用 server-side prepared statements
    # 适配 Supabase pooler/pgbouncer，彻底解决 DuplicatePreparedStatement / InvalidSqlStatementName
    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=20,
        timeout=30,
        kwargs={"prepare_threshold": 0},
    )

pool = get_pool()

def db_fetchone(sql, params=None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()

def db_fetchall(sql, params=None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def db_exec(sql, params=None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()

# =========================
# Init DB + Indexes
# =========================
def init_db():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                participant_id TEXT PRIMARY KEY,
                student_id TEXT,
                device TEXT,
                screen_resolution TEXT,
                start_time TIMESTAMPTZ
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS images (
                image_id TEXT PRIMARY KEY,
                rel_path TEXT NOT NULL,
                category INT NOT NULL,
                category_name TEXT,
                resolution TEXT NOT NULL,
                distortion INT NOT NULL,
                distortion_name TEXT,
                assigned_count INT NOT NULL DEFAULT 0
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS assignments (
                participant_id TEXT NOT NULL,
                image_id TEXT NOT NULL,
                ord INT NOT NULL,
                assigned_time TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (participant_id, image_id)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                participant_id TEXT,
                image_id TEXT,
                image_name TEXT,
                score INT,
                label TEXT,
                time TIMESTAMPTZ,
                text_clarity TEXT
            )
            """)

            # 索引：保证分配/查询快
            cur.execute("CREATE INDEX IF NOT EXISTS idx_images_axes ON images(category, resolution, distortion)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_images_assigned_count ON images(assigned_count)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_pid ON assignments(participant_id, ord)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ratings_pid ON ratings(participant_id)")

        conn.commit()

init_db()

# =========================
# Import manifest if needed
# =========================
def import_manifest_if_needed():
    r = db_fetchone("SELECT COUNT(*) FROM images")
    if r and int(r[0]) > 0:
        return

    if not os.path.exists(MANIFEST_CSV):
        st.error(f"找不到 {MANIFEST_CSV}，请确认它在仓库根目录。")
        st.stop()

    rows = []
    with open(MANIFEST_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for rr in reader:
            rows.append((
                rr["image_id"].strip(),
                rr["rel_path"].strip(),
                int(rr["category"]),
                (rr.get("category_name") or "").strip() or None,
                rr["resolution"].strip(),
                int(rr["distortion"]),
                (rr.get("distortion_name") or "").strip() or None,
            ))

    # 批量插入
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO images(image_id, rel_path, category, category_name, resolution, distortion, distortion_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (image_id) DO NOTHING
                """,
                rows
            )
        conn.commit()

import_manifest_if_needed()

# =========================
# Fast assignment (ONE SQL, atomic)
# =========================
def assign_images_for_participant(pid: str):
    # 已分配就不重复
    r = db_fetchone("SELECT COUNT(*) FROM assignments WHERE participant_id=%s", (pid,))
    if r and int(r[0]) >= K_PER_PERSON:
        return

    # 单次 SQL 做完：
    # 1) cover: 每个strata取前 COVER_M（用窗口函数）
    # 2) fill: 再补齐到 K_PER_PERSON
    # 3) upd: 先原子 +1 assigned_count（只对仍 < R_TARGET 的）
    # 4) ins: 插入 assignments（用 random() 生成 ord）
    sql = f"""
    WITH ranked AS (
      SELECT
        image_id,
        category, resolution, distortion,
        assigned_count,
        row_number() OVER (
          PARTITION BY category, resolution, distortion
          ORDER BY assigned_count ASC, image_id ASC
        ) AS rn
      FROM images
      WHERE assigned_count < %s
    ),
    cover AS (
      SELECT image_id
      FROM ranked
      WHERE rn <= %s
      LIMIT %s
    ),
    fill AS (
      SELECT image_id
      FROM images
      WHERE assigned_count < %s
        AND image_id NOT IN (SELECT image_id FROM cover)
      ORDER BY assigned_count ASC, image_id ASC
      LIMIT GREATEST(%s - (SELECT COUNT(*) FROM cover), 0)
    ),
    chosen AS (
      SELECT image_id FROM cover
      UNION ALL
      SELECT image_id FROM fill
    ),
    upd AS (
      UPDATE images i
      SET assigned_count = assigned_count + 1
      FROM chosen c
      WHERE i.image_id = c.image_id
        AND i.assigned_count < %s
      RETURNING i.image_id
    )
    INSERT INTO assignments(participant_id, image_id, ord, assigned_time)
    SELECT
      %s AS participant_id,
      u.image_id,
      (row_number() OVER (ORDER BY random()) - 1)::int AS ord,
      now() AS assigned_time
    FROM upd u
    ON CONFLICT DO NOTHING
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                sql,
                (
                    R_TARGET,
                    COVER_M,
                    K_PER_PERSON,
                    R_TARGET,
                    K_PER_PERSON,
                    R_TARGET,
                    pid,
                ),
            )
            conn.commit()

# =========================
# Session state
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "idx" not in st.session_state:
    st.session_state.idx = 0

# =========================
# Helpers
# =========================
def get_assigned_image_ids(pid: str):
    rows = db_fetchall(
        "SELECT image_id FROM assignments WHERE participant_id=%s ORDER BY ord ASC",
        (pid,),
    )
    return [r[0] for r in rows]

@st.cache_data(show_spinner=False)
def get_relpath_map(image_ids_tuple):
    # 一次性查一批，避免每张图都查一次 DB
    if not image_ids_tuple:
        return {}
    rows = db_fetchall(
        "SELECT image_id, rel_path FROM images WHERE image_id = ANY(%s)",
        (list(image_ids_tuple),),
    )
    return {r[0]: r[1] for r in rows}

def r2_url(rel_path: str) -> str:
    # rel_path 本身像 1080_M/Ebook/img_3.png
    return f"{R2_PUBLIC_BASE_URL}/{rel_path.lstrip('/')}"

# =========================
# Pages
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    with st.form("intro_form"):
        student_id = st.text_input("Student ID / 学号", "")
        device = st.selectbox("Device / 设备", ["PC / Laptop", "Tablet", "Phone", "Other"])

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

        resolution_choice = st.selectbox(
            "Screen Resolution / 请选择屏幕分辨率",
            ["1920×1080", "2560×1440", "3840×2160", "I don’t know (auto-detect)", "Other"],
        )

        if resolution_choice == "I don’t know (auto-detect)":
            st.caption(f"Auto-detected physical resolution: {detected_physical}")

        submitted = st.form_submit_button("Start Experiment")

    if not submitted:
        return
    if student_id.strip() == "":
        st.error("Please enter your student ID.")
        return

    if resolution_choice == "I don’t know (auto-detect)":
        screen_resolution = f"auto:{detected_physical or 'unknown'}"
    elif resolution_choice == "Other":
        screen_resolution = "manual:other"
    else:
        screen_resolution = f"manual:{resolution_choice.replace('×','x')}"

    pid = str(uuid4())
    st.session_state.participant_id = pid

    # 写 participants
    db_exec(
        """
        INSERT INTO participants(participant_id, student_id, device, screen_resolution, start_time)
        VALUES (%s,%s,%s,%s,now())
        """,
        (pid, student_id.strip(), device, screen_resolution),
    )

    # 分配（这里加 spinner，用户不会觉得卡死）
    with st.spinner("Assigning images... / 正在分配图片（首次会慢一点点）"):
        assign_images_for_participant(pid)

    st.session_state.stage = "training"
    st.rerun()

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
          以下示例仅用于帮助理解评分标准，不会记录分数。
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 训练图仍然用本地仓库里的 training_images（小文件，Render上也能读取）
    try:
        files = sorted([f for f in os.listdir(TRAIN_DIR) if not f.startswith(".")])[:5]
    except Exception:
        files = []
    if len(files) < 5:
        st.warning("training_images 下不足 5 张训练图，先跳过训练页也能评分。")

    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    if st.button("Next → Start Rating"):
        st.session_state.stage = "rating"
        st.session_state.idx = 0
        st.session_state.rating_start_ts = time.time()
        st.rerun()

def render_rating():
    pid = st.session_state.participant_id
    if not pid:
        st.error("No participant id.")
        st.stop()

    assigned_ids = get_assigned_image_ids(pid)
    total = len(assigned_ids)
    done = st.session_state.idx

    if total == 0:
        st.error("No assigned images. 分配失败：assignments 为空。")
        st.stop()

    if "rating_start_ts" not in st.session_state:
        st.session_state.rating_start_ts = time.time()

    elapsed = time.time() - st.session_state.rating_start_ts
    sec_per = elapsed / max(1, done)
    remaining_sec = max(0, (total - done) * sec_per)

    st.progress(done / total if total else 0, text=f"Progress: {done}/{total}")
    st.caption(f"Elapsed: {elapsed/60:.1f} min · Avg: {sec_per:.1f}s/img · ETA: {remaining_sec/60:.1f} min")

    if done >= total:
        st.session_state.stage = "done"
        st.rerun()
        return

    # 一次性缓存本参与者的 rel_path 映射（避免每张图都查 DB）
    rel_map = get_relpath_map(tuple(assigned_ids))
    image_id = assigned_ids[done]
    rel_path = rel_map.get(image_id)

    if not rel_path:
        st.error("Image rel_path missing in DB.")
        st.stop()

    left, right = st.columns([3.6, 1.4], gap="large")
    with left:
        if USE_R2:
            st.image(r2_url(rel_path), caption=rel_path, use_container_width=True)
        else:
            st.error("R2_PUBLIC_BASE_URL 未配置，无法在云端加载图片。")
            st.stop()

    with right:
        st.markdown("### Rate image quality")

        score = st.radio(
            "Quality score",
            options=[5, 4, 3, 2, 1],
            index=None,
            key=f"score_{done}",
            format_func=lambda x: f"{x} — {LABELS[x]}",
        )

        st.markdown("**Text clarity / 文本清晰度**")
        text_clarity = st.radio(
            "Text clarity",
            options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
            index=None,
            key=f"text_{done}",
        )

        next_clicked = st.button("Next", disabled=(score is None or text_clarity is None))

    if next_clicked:
        db_exec(
            """
            INSERT INTO ratings(participant_id, image_id, image_name, score, label, time, text_clarity)
            VALUES (%s,%s,%s,%s,%s,now(),%s)
            """,
            (pid, image_id, rel_path, int(score), LABELS[int(score)], str(text_clarity)),
        )
        st.session_state.idx += 1
        st.rerun()

def render_done():
    st.success("Thank you for participating! / 感谢参与！")
    st.write("You may now close this page.")

# Router
if st.session_state.stage == "intro":
    render_intro()
elif st.session_state.stage == "training":
    render_training()
elif st.session_state.stage == "rating":
    render_rating()
else:
    render_done()
