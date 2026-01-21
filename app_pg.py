# app_pg.py
# -*- coding: utf-8 -*-

import os
import csv
import time
import random
import io
import base64
from datetime import datetime
from uuid import uuid4
from PIL import Image

import streamlit as st
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval

import psycopg
from psycopg_pool import ConnectionPool

st.set_page_config(layout="wide")

# =========================
# ENV / Config
# =========================
DSN = os.environ.get("DATABASE_URL", "").strip()
if not DSN:
    st.error("缺少环境变量 DATABASE_URL（Supabase PostgreSQL 连接串）")
    st.stop()

R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)

# 训练图（你 GitHub 已经按这个放好了）
TRAIN_DIR = "training_images"
TRAIN_FILES = [
    "1bad.png",
    "2poor.png",
    "3fair.png",
    "4good.png",
    "5excellent.png",
]
TRAIN_INTERVAL_MS = 7000

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然",
}

# =========================
# DB Pool
# =========================
@st.cache_resource
def get_pool():
    # 不要往 DSN 里塞 prepare_threshold 等参数（你已经踩过坑）
    return ConnectionPool(conninfo=DSN, min_size=1, max_size=10, timeout=30)

pool = get_pool()

def pg_fetchall(sql, params=()):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params, prepare=False)
            return cur.fetchall()

def pg_fetchone(sql, params=()):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params, prepare=False)
            return cur.fetchone()

def pg_exec(sql, params=()):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params, prepare=False)
        conn.commit()

def ensure_schema():
    """
    ✅ 只做“确保存在/迁移”，不做清空
    - 关键：给旧 participants 自动补 slot 列（否则你一定会遇到 UndefinedColumn）
    - 确保 slot_counter / exp_config 存在
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # participants（包含 slot）
            cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                participant_id TEXT PRIMARY KEY,
                student_id TEXT,
                device TEXT,
                screen_resolution TEXT,
                start_time TIMESTAMP,
                slot INTEGER
            );
            """, prepare=False)

            # assignments
            cur.execute("""
            CREATE TABLE IF NOT EXISTS assignments (
                participant_id TEXT NOT NULL,
                image_id TEXT NOT NULL,
                ord INTEGER NOT NULL,
                assigned_time TIMESTAMP NOT NULL,
                PRIMARY KEY (participant_id, image_id)
            );
            """, prepare=False)

            # ratings
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                participant_id TEXT,
                image_id TEXT,
                image_name TEXT,
                score INTEGER,
                label TEXT,
                time TIMESTAMP,
                text_clarity TEXT
            );
            """, prepare=False)

            # 这些一般由你的 plan 脚本创建/导入，但这里兜底一下（不影响已有数据）
            cur.execute("""
            CREATE TABLE IF NOT EXISTS slot_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                next_slot INTEGER NOT NULL
            );
            """, prepare=False)
            cur.execute("""
            INSERT INTO slot_counter (id, next_slot)
            VALUES (1, 1)
            ON CONFLICT (id) DO NOTHING;
            """, prepare=False)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS exp_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                p_total INTEGER NOT NULL,
                r_target INTEGER NOT NULL,
                n_images INTEGER NOT NULL,
                k_per_person INTEGER NOT NULL,
                updated_at TIMESTAMP NOT NULL
            );
            """, prepare=False)

            # ✅ 自动迁移：如果旧 participants 没有 slot，就补上
            cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='participants' AND column_name='slot'
                ) THEN
                    ALTER TABLE participants ADD COLUMN slot INTEGER;
                END IF;
            END $$;
            """, prepare=False)

        conn.commit()

ensure_schema()

# =========================
# Helpers
# =========================
@st.cache_data(show_spinner=False)
def image_as_data_url(img_path: str, max_side: int = 2400, quality: int = 88) -> str:
    with Image.open(img_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"

def get_exp_config():
    r = pg_fetchone("SELECT n_images, k_per_person, p_total, r_target FROM exp_config WHERE id=1")
    if not r:
        st.error("数据库缺少 exp_config（你需要先运行 make_assignment_plan_from_manifest.py 导入）")
        st.stop()
    n_images, k_per, p_total, r_target = r
    return int(n_images), int(k_per), int(p_total), int(r_target)

def allocate_next_slot(p_total: int) -> int:
    """
    ✅ 原子发号（强一致，不卡）
    - 取出当前 next_slot（就是本次分配给用户的 slot）
    - 然后把 next_slot 更新为下一位（循环 1..P）
    - 用 FOR UPDATE 锁定这一行，避免并发冲突
    """
    if p_total <= 0:
        raise ValueError("p_total must be > 0")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH s AS (
                    SELECT next_slot
                    FROM slot_counter
                    WHERE id=1
                    FOR UPDATE
                ),
                u AS (
                    UPDATE slot_counter
                    SET next_slot = (s.next_slot %% %s) + 1
                    FROM s
                    WHERE id=1
                    RETURNING s.next_slot AS slot_assigned
                )
                SELECT slot_assigned FROM u;
                """,
                (p_total,),
                prepare=False
            )
            row = cur.fetchone()
        conn.commit()

    if not row or row[0] is None:
        # 极端情况兜底
        return 1
    return int(row[0])

def get_plan_image_ids_for_slot(slot: int):
    rows = pg_fetchall(
        "SELECT image_id FROM assignment_plan WHERE slot=%s ORDER BY ord ASC",
        (slot,)
    )
    return [r[0] for r in rows]

def get_rel_path(image_id: str):
    r = pg_fetchone("SELECT rel_path FROM images WHERE image_id=%s", (image_id,))
    return r[0] if r else None

def get_assigned_image_ids(pid: str):
    rows = pg_fetchall(
        "SELECT image_id FROM assignments WHERE participant_id=%s ORDER BY ord ASC",
        (pid,)
    )
    return [r[0] for r in rows]

def assign_images_for_participant(pid: str, slot: int):
    """
    ✅ 不用 executemany（避免 prepared statement 冲突）
    ✅ 全部 prepare=False
    """
    exist = pg_fetchone(
        "SELECT 1 FROM assignments WHERE participant_id=%s LIMIT 1",
        (pid,)
    )
    if exist:
        return

    image_ids = get_plan_image_ids_for_slot(slot)
    if not image_ids:
        st.error(f"assignment_plan 里找不到 slot={slot} 的数据（请检查 plan 导入）")
        st.stop()

    now = datetime.now()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for ord_i, image_id in enumerate(image_ids):
                cur.execute(
                    """
                    INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (pid, image_id, ord_i, now),
                    prepare=False
                )
        conn.commit()

# =========================
# Session State
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "slot" not in st.session_state:
    st.session_state.slot = None
if "idx" not in st.session_state:
    st.session_state.idx = 0

# =========================
# Pages
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    n_images, k_per, p_total, r_target = get_exp_config()
    st.caption(f"Experiment config: N={n_images}, K/person={k_per}, P={p_total}, R_target={r_target}")

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

    # ✅ 原子发 slot
    slot = allocate_next_slot(p_total)

    # 写 participants（包含 slot）
    pg_exec(
        """
        INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time, slot)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (pid, student_id.strip(), device, screen_resolution, datetime.now(), slot)
    )

    # 写 assignments（来自 assignment_plan）
    assign_images_for_participant(pid, slot)

    st.session_state.participant_id = pid
    st.session_state.slot = slot
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
          以下示例仅用于帮助理解评分标准，不会记录分数。培训完请点击下方按钮开始打分。
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ✅ 固定顺序：按 TRAIN_FILES
    paths = [os.path.join(TRAIN_DIR, f) for f in TRAIN_FILES]
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        st.error("training_images 缺少文件：\n" + "\n".join(missing))
        st.stop()

    urls = [image_as_data_url(p, max_side=2400, quality=88) for p in paths]
    caps = [
        f"1 — {LABELS[1]}",
        f"2 — {LABELS[2]}",
        f"3 — {LABELS[3]}",
        f"4 — {LABELS[4]}",
        f"5 — {LABELS[5]}",
    ]

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

def render_rating():
    pid = st.session_state.participant_id
    if not pid:
        st.error("No participant id.")
        st.stop()

    assigned_ids = get_assigned_image_ids(pid)
    total = len(assigned_ids)
    done = st.session_state.idx

    if total == 0:
        st.error("该参与者 assignments 为空（可能 assign_images_for_participant 没执行成功）")
        st.stop()

    if "rating_start_ts" not in st.session_state:
        st.session_state.rating_start_ts = time.time()

    elapsed = time.time() - st.session_state.rating_start_ts
    sec_per = elapsed / max(1, done)
    remaining_sec = max(0, (total - done) * sec_per)

    st.progress(done / total, text=f"Progress: {done}/{total} images completed")
    st.caption(f"Elapsed: {elapsed/60:.1f} min · Avg: {sec_per:.1f}s/image · ETA: {remaining_sec/60:.1f} min")

    if done >= total:
        st.session_state.stage = "done"
        st.rerun()
        return

    image_id = assigned_ids[done]
    rel_path = get_rel_path(image_id)
    if not rel_path:
        st.error(f"images 表里找不到 image_id={image_id}")
        st.stop()

    left, right = st.columns([3.6, 1.4], gap="large")

    with left:
        if USE_R2:
            img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"
            st.image(img_url, caption=rel_path, use_container_width=True)
        else:
            st.error("缺少 R2_PUBLIC_BASE_URL（线上必须走 R2）")
            st.stop()

    with right:
        st.markdown("### Rate image quality")

        score = st.radio(
            "",
            options=[5, 4, 3, 2, 1],
            index=None,
            key=f"score_{done}",
            format_func=lambda x: f"{x} — {LABELS[x]}",
            label_visibility="collapsed",
        )

        st.markdown("**Text clarity / 文本清晰度**")
        text_clarity = st.radio(
            "",
            options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
            index=None,
            key=f"text_{done}",
            label_visibility="collapsed",
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
else:
    render_done()
