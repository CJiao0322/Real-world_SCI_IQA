import os
import time
import random
import logging
import csv
from uuid import uuid4
from datetime import datetime

import streamlit as st
import psycopg
from psycopg_pool import ConnectionPool

# =========================
# Logging (Render Logs 能看到)
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

st.set_page_config(layout="wide")

# =========================
# ENV / Config
# =========================
# 1) Render 环境变量里设置 DATABASE_URL（Supabase 的那个）
DSN = os.environ.get("DATABASE_URL", "").strip()
if not DSN:
    st.error("Missing env var DATABASE_URL")
    st.stop()

# ✅ 关键：禁用 psycopg 自动 prepared statements，避免 Supabase pooler 报错
# psycopg3 支持 prepare_threshold 参数（放在 conninfo 里）
# 若 DSN 是 URL 形式：追加 prepare_threshold=0
if "prepare_threshold=" not in DSN:
    if "?" in DSN:
        DSN = DSN + "&prepare_threshold=0"
    else:
        DSN = DSN + "?prepare_threshold=0"

# 2) R2 公网前缀（你已经在 Render 里加了）
# 例如：https://<accountid>.r2.cloudflarestorage.com/sci-iqa-images
R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)

# 你的实验参数
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
# PG connection pool
# =========================
@st.cache_resource
def get_pool():
    # max_size 别太大；Render 单实例 + DB pooler，太大反而抖
    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=8,
        timeout=30,
        max_idle=60,
    )

pool = get_pool()

def pg_exec(sql, params=None, fetch=False, fetchone=False):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetchone:
                return cur.fetchone()
            if fetch:
                return cur.fetchall()
        conn.commit()

# =========================
# Speed: Assign images in 2~3 SQL calls
# =========================
def assign_images_for_participant(pid: str):
    """
    快速并发安全分配：
    - 一次 SQL 用窗口函数取每个 strata 的 COVER_M
    - 再补齐到 K_PER_PERSON
    - FOR UPDATE SKIP LOCKED 并发安全
    - 禁用 prepared statements 后不会再 DuplicatePreparedStatement
    """
    # 已有分配则跳过
    row = pg_exec("SELECT COUNT(*) FROM assignments WHERE participant_id=%s", (pid,), fetchone=True)
    already = int(row[0]) if row else 0
    if already >= K_PER_PERSON:
        return

    now = datetime.now()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")

            # 1) coverage + fill 一次性挑选候选集（再由 python 去重/裁剪）
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

            # 去重 + 裁剪
            seen = set()
            chosen = [x for x in chosen if not (x in seen or seen.add(x))]
            if len(chosen) > K_PER_PERSON:
                chosen = chosen[:K_PER_PERSON]

            # 2) assignments 批量写入
            cur.executemany(
                """
                INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
            )

            # 3) assigned_count 一次性更新（比 executemany 快很多）
            cur.execute(
                """
                UPDATE images
                SET assigned_count = assigned_count + 1
                WHERE image_id = ANY(%s)
                """,
                (chosen,)
            )

            conn.commit()

# =========================
# Prefetch: rating 页不再每张图查 DB
# =========================
def prefetch_assignments(pid: str):
    rows = pg_exec(
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
    return rows  # [(image_id, rel_path), ...]

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

    # 创建参与者
    pid = str(uuid4())
    st.session_state.participant_id = pid

    pg_exec(
        """
        INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (pid, student_id.strip(), device, resolution_choice, datetime.now())
    )

    # 分配（打印耗时到 Render Logs）
    t0 = time.time()
    log.info("Start assign pid=%s", pid)
    assign_images_for_participant(pid)
    log.info("Assign finished in %.2fs", time.time() - t0)

    # 预取 assignments（评分阶段不再反复查库）
    t1 = time.time()
    st.session_state.assigned = prefetch_assignments(pid)
    log.info("Prefetch assignments done: %d in %.2fs", len(st.session_state.assigned), time.time() - t1)

    # 进入评分
    st.session_state.stage = "rating"
    st.session_state.idx = 0
    st.session_state.rating_start_ts = time.time()
    st.rerun()

def render_rating():
    pid = st.session_state.participant_id
    pairs = st.session_state.get("assigned", [])

    if not pid:
        st.error("No participant id.")
        st.stop()

    if not pairs:
        st.warning("No assignments found. Retrying prefetch...")
        st.session_state.assigned = prefetch_assignments(pid)
        pairs = st.session_state.assigned
        if not pairs:
            st.error("Still no assignments. Please check DB assignments/images.")
            st.stop()

    total = len(pairs)
    done = st.session_state.idx

    if "rating_start_ts" not in st.session_state:
        st.session_state.rating_start_ts = time.time()

    elapsed = time.time() - st.session_state.rating_start_ts
    sec_per = elapsed / max(1, done)
    remaining_sec = max(0, (total - done) * sec_per)

    st.progress(done / total if total else 0, text=f"Progress: {done}/{total} images completed")
    st.caption(f"Elapsed: {elapsed/60:.1f} min · Avg: {sec_per:.1f}s/image · ETA: {remaining_sec/60:.1f} min")

    if done >= total:
        st.session_state.stage = "done"
        st.rerun()
        return

    image_id, rel_path = pairs[done]

    # 图片 URL（R2）
    if USE_R2:
        img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"
        left, right = st.columns([3.6, 1.4], gap="large")
        with left:
            st.image(img_url, caption=rel_path, use_container_width=True)
    else:
        st.error("USE_R2 is False: Please set R2_PUBLIC_BASE_URL on Render.")
        st.stop()

    with right:
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
        # 写入评分（一次 SQL）
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
    st.success("Thank you for participating! / 感谢参与！")
    st.write("You may now close this page.")

# =========================
# Router
# =========================
if st.session_state.stage == "intro":
    render_intro()
elif st.session_state.stage == "rating":
    render_rating()
else:
    render_done()
