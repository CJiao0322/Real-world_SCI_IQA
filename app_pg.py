import os
import time
import random
import logging
from uuid import uuid4
from datetime import datetime

import streamlit as st
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
    st.warning("R2_PUBLIC_BASE_URL not set. Images may not load.")

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
# Pool (关键：不要往 DSN 里塞 prepare_threshold)
# 同时：pool 不要太大，Render 单实例 + Supabase pooler 很容易卡死
# =========================
@st.cache_resource
def get_pool():
    # ✅ after_connect：每次拿到新连接都执行，禁用 prepared statement 缓存
    # 这样不会碰 DSN 解析问题
    def _after_connect(conn):
        # 对 Supabase / PgBouncer 这类 pooled 连接，prepared statements 经常出问题
        # 这两句足够把“重复/不存在 prepared statement”概率打到很低
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30s';")
            cur.execute("SET idle_in_transaction_session_timeout = '30s';")
            # 关闭服务端 prepared statement 的使用（psycopg3 的话靠减少缓存/避免计划复用）
            # 这里不写 prepare_threshold，直接用 DEFERRED/自适应方案：禁用 session 级缓存
            cur.execute("SET plan_cache_mode = force_generic_plan;")
        conn.commit()

    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=3,          # ✅ 先小一点，避免 pooler/Render 卡死
        timeout=60,          # ✅ 给连接更久时间
        max_idle=30,
        reconnect_timeout=5,
        num_workers=1,       # ✅ 避免开很多后台线程抢连接
        after_connect=_after_connect,
    )

pool = get_pool()

def pg_exec(sql, params=None, fetch=False, fetchone=False):
    # 每次操作都打印耗时，方便你在 Render logs 定位慢点
    t0 = time.time()
    with pool.connection(timeout=60) as conn:
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
# Fast assign (2~3 次SQL)
# =========================
def assign_images_for_participant(pid: str):
    row = pg_exec("SELECT COUNT(*) FROM assignments WHERE participant_id=%s", (pid,), fetchone=True)
    already = int(row[0]) if row else 0
    if already >= K_PER_PERSON:
        return

    now = datetime.now()

    with pool.connection(timeout=60) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")

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
                  SELECT image_id
                  FROM ranked
                  WHERE rn <= %s
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

            seen = set()
            chosen = [x for x in chosen if not (x in seen or seen.add(x))]
            if len(chosen) > K_PER_PERSON:
                chosen = chosen[:K_PER_PERSON]

            cur.executemany(
                """
                INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
            )

            cur.execute(
                """
                UPDATE images
                SET assigned_count = assigned_count + 1
                WHERE image_id = ANY(%s)
                """,
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

    # 先测试数据库连通性（会打印 SQL 耗时）
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

    # assign
    t0 = time.time()
    assign_images_for_participant(pid)
    log.info("Assign finished %.2fs", time.time() - t0)

    # prefetch
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
            img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"
            st.image(img_url, caption=rel_path, use_container_width=True)
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
