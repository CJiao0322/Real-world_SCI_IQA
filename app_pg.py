import os
import time
import logging
from uuid import uuid4
from datetime import datetime

import streamlit as st
import psycopg
from psycopg_pool import ConnectionPool

# =========================
# Logging
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

st.set_page_config(layout="wide")

# =========================
# ENV
# =========================
DSN = os.environ.get("DATABASE_URL", "").strip()
if not DSN:
    st.error("Missing env var DATABASE_URL")
    st.stop()

R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)
if not USE_R2:
    st.warning("R2_PUBLIC_BASE_URL not set. Images may not load from R2.")

P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P  # 500
COVER_M = 2

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然"
}

# =========================
# Pool  (保守，避免 Render + Supabase pooler 卡死)
# =========================
@st.cache_resource
def get_pool():
    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=3,      # ✅ 小一点更稳，避免 pooler 抽风
        timeout=60,
    )

pool = get_pool()

# =========================
# Connection init per-use  ✅关键：每次拿到连接就跑一下初始化SQL
# 不需要 after_connect，也不需要往 DSN 里加 prepare_threshold
# =========================
INIT_SQL = """
SET statement_timeout = '30s';
SET idle_in_transaction_session_timeout = '30s';
SET plan_cache_mode = force_generic_plan;
"""

def _init_conn(conn):
    # 每次从池子拿到 conn，轻量初始化（几毫秒）
    try:
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
        conn.commit()
    except Exception:
        # 初始化失败也别让连接挂住
        try:
            conn.rollback()
        except Exception:
            pass

def pg_exec(sql, params=None, fetch=False, fetchone=False):
    t0 = time.time()
    with pool.connection(timeout=60) as conn:
        _init_conn(conn)  # ✅ 在这里做 init

        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetchone:
                res = cur.fetchone()
            elif fetch:
                res = cur.fetchall()
            else:
                res = None

        conn.commit()

    log.info("SQL %.2fs | %s", time.time() - t0, sql.strip().splitlines()[0][:120])
    return res

# =========================
# Assignment: 一次性 join 取 rel_path，避免 rating 页面每张图查DB
# =========================
def assign_images_for_participant(pid: str):
    row = pg_exec("SELECT COUNT(*) FROM assignments WHERE participant_id=%s", (pid,), fetchone=True)
    already = int(row[0]) if row else 0
    if already >= K_PER_PERSON:
        return

    now = datetime.now()

    with pool.connection(timeout=60) as conn:
        _init_conn(conn)
        with conn.cursor() as cur:
            cur.execute("BEGIN")

            # ✅ 用窗口函数做 coverage + fill，SQL 数量很少
            cur.execute(
                """
                WITH ranked AS (
                  SELECT
                    image_id,
                    row_number() OVER (
                      PARTITION BY category, resolution, distortion
                      ORDER BY assigned_count ASC, image_id ASC
                    ) AS rn
                  FROM images
                  WHERE assigned_count < %s
                ),
                coverage AS (
                  SELECT image_id FROM ranked WHERE rn <= %s
                ),
                fill AS (
                  SELECT image_id
                  FROM images
                  WHERE assigned_count < %s
                    AND image_id NOT IN (SELECT image_id FROM coverage)
                  ORDER BY assigned_count ASC, image_id ASC
                  LIMIT %s
                ),
                picked AS (
                  SELECT image_id FROM coverage
                  UNION ALL
                  SELECT image_id FROM fill
                )
                SELECT i.image_id
                FROM images i
                JOIN picked p ON p.image_id = i.image_id
                FOR UPDATE SKIP LOCKED
                """,
                (R_TARGET, COVER_M, R_TARGET, K_PER_PERSON)
            )
            chosen = [r[0] for r in cur.fetchall()]

            # 去重 + 截断
            seen = set()
            chosen = [x for x in chosen if not (x in seen or seen.add(x))]
            if len(chosen) > K_PER_PERSON:
                chosen = chosen[:K_PER_PERSON]

            # 写入 assignments
            cur.executemany(
                """
                INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
            )

            # 更新 assigned_count
            cur.execute(
                "UPDATE images SET assigned_count = assigned_count + 1 WHERE image_id = ANY(%s)",
                (chosen,)
            )

            conn.commit()

def prefetch_assignments(pid: str):
    return pg_exec(
        """
        SELECT a.image_id, i.rel_path
        FROM assignments a
        JOIN images i ON i.image_id = a.image_id
        WHERE a.participant_id=%s
        ORDER BY a.ord ASC
        """,
        (pid,),
        fetch=True
    )

# =========================
# Session init
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "assigned" not in st.session_state:
    st.session_state.assigned = []
if "rating_start_ts" not in st.session_state:
    st.session_state.rating_start_ts = None

# =========================
# Pages
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    with st.form("intro_form"):
        student_id = st.text_input("Student ID / 学号", "")
        device = st.selectbox("Device / 设备", ["PC / Laptop", "Tablet", "Phone", "Other"])
        resolution_choice = st.selectbox(
            "Screen Resolution / 请选择屏幕分辨率",
            ["1920×1080", "2560×1440", "3840×2160", "I don’t know", "Other"],
        )
        submitted = st.form_submit_button("Start Experiment")

    if not submitted:
        return
    if student_id.strip() == "":
        st.error("Please enter your student ID.")
        return

    pid = str(uuid4())
    st.session_state.participant_id = pid

    # ✅ 先测连接（日志里会显示耗时）
    try:
        pg_exec("SELECT 1", fetchone=True)
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        st.stop()

    # 写 participants
    pg_exec(
        """
        INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (pid, student_id.strip(), device, resolution_choice, datetime.now())
    )

    # assign + prefetch
    t0 = time.time()
    assign_images_for_participant(pid)
    log.info("Assign finished %.2fs", time.time() - t0)

    t1 = time.time()
    st.session_state.assigned = prefetch_assignments(pid)
    log.info("Prefetch %d finished %.2fs", len(st.session_state.assigned), time.time() - t1)

    st.session_state.stage = "rating"
    st.session_state.idx = 0
    st.session_state.rating_start_ts = time.time()
    st.rerun()

def render_rating():
    pid = st.session_state.participant_id
    pairs = st.session_state.assigned

    if not pid:
        st.error("No participant id.")
        st.stop()

    if not pairs:
        st.warning("No assignments found. Retrying...")
        st.session_state.assigned = prefetch_assignments(pid)
        pairs = st.session_state.assigned
        if not pairs:
            st.error("Still no assignments. Check DB.")
            st.stop()

    total = len(pairs)
    done = st.session_state.idx

    elapsed = time.time() - (st.session_state.rating_start_ts or time.time())
    sec_per = elapsed / max(1, done)
    remaining_sec = max(0, (total - done) * sec_per)

    st.progress(done / total if total else 0, text=f"Progress: {done}/{total}")
    st.caption(f"Elapsed: {elapsed/60:.1f} min · Avg: {sec_per:.1f}s/img · ETA: {remaining_sec/60:.1f} min")

    if done >= total:
        st.session_state.stage = "done"
        st.rerun()
        return

    image_id, rel_path = pairs[done]

    left, right = st.columns([3.6, 1.4], gap="large")
    with left:
        if USE_R2:
            st.image(f"{R2_PUBLIC_BASE_URL}/{rel_path}", caption=rel_path, use_container_width=True)
        else:
            st.error("R2_PUBLIC_BASE_URL not set.")
            st.stop()

    with right:
        score = st.radio(
            "Quality score",
            options=[5, 4, 3, 2, 1],
            index=None,
            key=f"score_{done}",
            format_func=lambda x: f"{x} — {LABELS[x]}",
        )
        text_clarity = st.radio(
            "Text clarity / 文本清晰度",
            options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
            index=None,
            key=f"text_{done}",
        )

        next_clicked = st.button("Next", disabled=(score is None or text_clarity is None))

    if next_clicked:
        pg_exec(
            """
            INSERT INTO ratings (participant_id, image_id, image_name, score, label, time, text_clarity)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (pid, image_id, rel_path, int(score), LABELS[int(score)], datetime.now(), str(text_clarity))
        )
        st.session_state.idx += 1
        st.rerun()

def render_done():
    st.success("Thank you! / 感谢参与！")
    st.write("You may close this page.")

# =========================
# Router
# =========================
if st.session_state.stage == "intro":
    render_intro()
elif st.session_state.stage == "rating":
    render_rating()
else:
    render_done()
