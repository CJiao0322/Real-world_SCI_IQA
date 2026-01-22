# app_pg.py
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime
from uuid import uuid4

import streamlit as st
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval

import psycopg
from psycopg_pool import ConnectionPool
from psycopg_pool import PoolTimeout

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

TRAIN_DIR = "training_images"
TRAIN_FILES = [
    "1bad.png",
    "2poor.bmp",
    "3fair.png",
    "4good.png",
    "5excellent.png",
]

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然",
}

# =========================
# DB tuning knobs
# =========================
# 关键：Supabase（尤其 free）不适合开很大的真实 PG 连接数
POOL_MAX_SIZE = int(os.environ.get("PG_POOL_MAX_SIZE", "4"))
POOL_TIMEOUT_SEC = float(os.environ.get("PG_POOL_TIMEOUT_SEC", "8"))
CONNECT_TIMEOUT_SEC = int(os.environ.get("PG_CONNECT_TIMEOUT_SEC", "5"))

# =========================
# One-time schema check (NO POOL)
# =========================
@st.cache_resource
def ensure_schema_once():
    """
    ✅ 只跑一次
    ✅ 不依赖 psycopg_pool（避免启动阶段 pool 就卡死）
    """
    with psycopg.connect(DSN, connect_timeout=CONNECT_TIMEOUT_SEC) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                participant_id TEXT PRIMARY KEY,
                student_id TEXT,
                device TEXT,
                screen_resolution TEXT,
                start_time TIMESTAMP,
                slot INTEGER
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS assignments (
                participant_id TEXT NOT NULL,
                image_id TEXT NOT NULL,
                ord INTEGER NOT NULL,
                assigned_time TIMESTAMP NOT NULL,
                PRIMARY KEY (participant_id, image_id)
            );
            """)

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
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS slot_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                next_slot INTEGER NOT NULL
            );
            """)
            cur.execute("""
            INSERT INTO slot_counter (id, next_slot)
            VALUES (1, 1)
            ON CONFLICT (id) DO NOTHING;
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS exp_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                p_total INTEGER NOT NULL,
                r_target INTEGER NOT NULL,
                n_images INTEGER NOT NULL,
                k_per_person INTEGER NOT NULL,
                updated_at TIMESTAMP NOT NULL
            );
            """)

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
            """)
        conn.commit()

ensure_schema_once()

# =========================
# DB Pool (shared)
# =========================
@st.cache_resource
def get_pool():
    # ✅ 小池 + 短超时：避免把 Supabase 撑爆
    # ✅ connect_timeout：避免握手卡住占用连接
    return ConnectionPool(
        conninfo=DSN,
        min_size=2,
        max_size=POOL_MAX_SIZE,
        timeout=POOL_TIMEOUT_SEC,
        kwargs={"connect_timeout": CONNECT_TIMEOUT_SEC},
    )

pool = get_pool()

def _execute_with_fallback(fetch: str, sql: str, params=()):
    """
    fetch:
      - "one": fetchone
      - "all": fetchall
      - "none": no fetch, commit
    关键修复：
    1) 先尝试 pool.connection()
    2) 如果 PoolTimeout / 连接异常：降级为 direct psycopg.connect 执行一次
    """
    # --- 1) try pool ---
    try:
        with pool.connection(timeout=POOL_TIMEOUT_SEC) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params, prepare=False)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
            # fetch == "none"
            conn.commit()
            return None
    except PoolTimeout:
        # --- 2) fallback direct connect ---
        with psycopg.connect(DSN, connect_timeout=CONNECT_TIMEOUT_SEC) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params, prepare=False)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
            conn.commit()
            return None
    except psycopg.Error:
        # pool 里的某些连接坏掉/断开时，直接降级也能救
        with psycopg.connect(DSN, connect_timeout=CONNECT_TIMEOUT_SEC) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params, prepare=False)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
            conn.commit()
            return None

def pg_fetchall(sql, params=()):
    return _execute_with_fallback("all", sql, params)

def pg_fetchone(sql, params=()):
    return _execute_with_fallback("one", sql, params)

def pg_exec(sql, params=()):
    _execute_with_fallback("none", sql, params)

# =========================
# Core helpers
# =========================
@st.cache_data(ttl=10, show_spinner=False)
def get_exp_config_cached():
    r = pg_fetchone("SELECT n_images, k_per_person, p_total, r_target FROM exp_config WHERE id=1")
    return r

def get_exp_config():
    r = get_exp_config_cached()
    if not r:
        st.error("数据库缺少 exp_config（你需要先运行 make_assignment_plan_from_manifest.py 导入）")
        st.stop()
    n_images, k_per, p_total, r_target = r
    return int(n_images), int(k_per), int(p_total), int(r_target)

def allocate_next_slot(p_total: int) -> int:
    if p_total <= 0:
        return 1
    row = pg_fetchone(
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
        (p_total,)
    )
    return int(row[0]) if row and row[0] is not None else 1

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
    ords = list(range(len(image_ids)))

    pg_exec(
        """
        INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
        SELECT %s, x.image_id, x.ord, %s
        FROM unnest(%s::text[], %s::int[]) AS x(image_id, ord)
        ON CONFLICT DO NOTHING
        """,
        (pid, now, image_ids, ords)
    )

def get_existing_participant_by_student(student_id: str):
    row = pg_fetchone(
        """
        SELECT p.participant_id, p.slot
        FROM participants p
        JOIN (
          SELECT participant_id, COUNT(*) AS n_assign
          FROM assignments
          GROUP BY participant_id
        ) a ON a.participant_id = p.participant_id
        LEFT JOIN (
          SELECT participant_id, COUNT(*) AS n_rate
          FROM ratings
          GROUP BY participant_id
        ) r ON r.participant_id = p.participant_id
        WHERE p.student_id = %s
          AND COALESCE(r.n_rate, 0) < a.n_assign
        ORDER BY p.start_time DESC
        LIMIT 1
        """,
        (student_id,)
    )
    if row:
        return row[0], int(row[1])

    row2 = pg_fetchone(
        """
        SELECT participant_id, slot
        FROM participants
        WHERE student_id=%s
        ORDER BY start_time DESC
        LIMIT 1
        """,
        (student_id,)
    )
    if row2:
        return row2[0], int(row2[1])

    return None

def get_progress(pid: str):
    done = pg_fetchone("SELECT COUNT(*) FROM ratings WHERE participant_id=%s", (pid,))[0]
    total = pg_fetchone("SELECT COUNT(*) FROM assignments WHERE participant_id=%s", (pid,))[0]
    return int(done), int(total)

def restore_session(pid: str, slot: int):
    done, total = get_progress(pid)
    st.session_state.participant_id = pid
    st.session_state.slot = slot
    st.session_state.idx = done
    st.session_state.stage = "rating" if done < total else "done"

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

    try:
        n_images, k_per, p_total, r_target = get_exp_config()
    except Exception as e:
        st.error("数据库暂时连接不上（可能 Supabase 连接被占满/网络抖动）。请刷新页面再试。")
        st.code(repr(e))
        st.stop()

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

    sid = student_id.strip()

    existing = get_existing_participant_by_student(sid)
    if existing:
        old_pid, old_slot = existing
        done, total = get_progress(old_pid)
        if total > 0 and done < total:
            st.success(f"检测到你之前未完成的进度：{done}/{total}，已为你继续。")
            restore_session(old_pid, old_slot)
            st.rerun()
            return
        st.info("检测到你之前已经完成过本实验。本次将开始新一轮。")

    pid = str(uuid4())
    slot = allocate_next_slot(p_total)

    pg_exec(
        """
        INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time, slot)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (pid, sid, device, screen_resolution, datetime.now(), slot)
    )

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

    caps = [
        f"1 — {LABELS[1]}",
        f"2 — {LABELS[2]}",
        f"3 — {LABELS[3]}",
        f"4 — {LABELS[4]}",
        f"5 — {LABELS[5]}",
    ]

    if not USE_R2:
        st.error("Training 阶段需要 R2_PUBLIC_BASE_URL（建议走 R2）")
        st.stop()

    if "train_idx" not in st.session_state:
        st.session_state.train_idx = 0

    colA, _, colC = st.columns([1, 6, 1], vertical_alignment="center")
    with colA:
        if st.button("← Prev", use_container_width=True):
            st.session_state.train_idx = (st.session_state.train_idx - 1) % len(TRAIN_FILES)
            st.rerun()
    with colC:
        if st.button("Next →", use_container_width=True):
            st.session_state.train_idx = (st.session_state.train_idx + 1) % len(TRAIN_FILES)
            st.rerun()

    idx = st.session_state.train_idx
    st.markdown(
        f"<div style='text-align:center; font-size:22px; font-weight:950;'>{caps[idx]}</div>",
        unsafe_allow_html=True
    )

    base = f"{R2_PUBLIC_BASE_URL}/{TRAIN_DIR}"
    cur_url = f"{base}/{TRAIN_FILES[idx]}"
    next_url = f"{base}/{TRAIN_FILES[(idx + 1) % len(TRAIN_FILES)]}"
    prev_url = f"{base}/{TRAIN_FILES[(idx - 1) % len(TRAIN_FILES)]}"

    # ✅ 无滚动：一屏展示（contain），限制宽度，限制高度
    components.html(
        f"""
        <head>
          <link rel="preload" as="image" href="{next_url}">
          <link rel="preload" as="image" href="{prev_url}">
        </head>
        <div style="
            width:100%;
            height:78vh;
            border:1px solid #eee;
            border-radius:8px;
            display:flex;
            justify-content:center;
            align-items:center;
            background:#fafafa;
            overflow:hidden;
        ">
          <img src="{cur_url}"
               style="max-width:1600px; max-height:100%; width:auto; height:auto; object-fit:contain;"
               decoding="async"
               loading="eager"
          />
        </div>
        """,
        height=820,
    )

    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

    if st.button("Next → Start Rating"):
        st.session_state.stage = "rating"
        st.session_state.idx = 0
        st.session_state.rating_start_ts = time.time()
        if "train_idx" in st.session_state:
            del st.session_state["train_idx"]
        st.rerun()

def render_rating():
    pid = st.session_state.participant_id
    if not pid:
        st.error("No participant id.")
        st.stop()

    # ✅ 缓存 assignments，减少 DB 压力
    if "assigned_ids" not in st.session_state or st.session_state.get("assigned_pid") != pid:
        st.session_state.assigned_ids = get_assigned_image_ids(pid)
        st.session_state.assigned_pid = pid

    assigned_ids = st.session_state.assigned_ids
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
    rel_key = f"rel_{image_id}"
    if rel_key not in st.session_state:
        st.session_state[rel_key] = get_rel_path(image_id)
    rel_path = st.session_state[rel_key]

    if not rel_path:
        st.error(f"images 表里找不到 image_id={image_id}")
        st.stop()

    left, right = st.columns([3.6, 1.4], gap="large")

    with left:
        if USE_R2:
            img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"

            img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"

            # 预取下一张（如果有）
            next_url = None
            if done + 1 < total:
                next_id = assigned_ids[done + 1]
                next_rel = st.session_state.get(f"rel_{next_id}")
                if not next_rel:
                    next_rel = get_rel_path(next_id)
                    st.session_state[f"rel_{next_id}"] = next_rel
                if next_rel:
                    next_url = f"{R2_PUBLIC_BASE_URL}/{next_rel}"

            # st.image(img_url, caption=rel_path, use_container_width=True)
            preload_html = ""
            if next_url:
                preload_html = f'<link rel="preload" as="image" href="{next_url}">'
            
            components.html(
                f"""
                <head>
                  {preload_html}
                </head>
                <div style="
                    width:100%;
                    height:78vh;
                    border:1px solid #eee;
                    border-radius:8px;
                    display:flex;
                    justify-content:center;
                    align-items:center;
                    background:#fafafa;
                    overflow:hidden;
                ">
                  <img src="{img_url}"
                       style="max-width:1600px; max-height:100%; width:auto; height:auto; object-fit:contain;"
                       decoding="async"
                       loading="eager"
                  />
                </div>
                <div style="font-size:12px; opacity:0.7; margin-top:6px;">{rel_path}</div>
                """,
                height=860,
            )

        else:
            st.error("缺少 R2_PUBLIC_BASE_URL（线上必须走 R2）")
            st.stop()

    # with right:
    #     st.markdown("### Rate image quality")

    #     with st.form(key=f"rate_form_{done}", clear_on_submit=True):
    #         score = st.radio(
    #             "",
    #             options=[5, 4, 3, 2, 1],
    #             index=None,
    #             format_func=lambda x: f"{x} — {LABELS[x]}",
    #             label_visibility="collapsed",
    #         )

    #         st.markdown("**Text clarity / 文本清晰度**")
    #         text_clarity = st.radio(
    #             "",
    #             options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
    #             index=None,
    #             label_visibility="collapsed",
    #         )

    #         submitted = st.form_submit_button("Next", disabled=(score is None or text_clarity is None))

    #     if submitted:
    #         pg_exec(
    #             """
    #             INSERT INTO ratings (participant_id, image_id, image_name, score, label, time, text_clarity)
    #             VALUES (%s,%s,%s,%s,%s,%s,%s)
    #             """,
    #             (pid, image_id, rel_path, int(score), LABELS[int(score)], datetime.now(), str(text_clarity))
    #         )
    #         st.session_state.idx += 1
    #         st.rerun()
    with right:
        st.markdown("### Rate image quality")
    
        with st.form(key=f"rate_form_{done}", clear_on_submit=True):
            score = st.radio(
                "",
                options=[5, 4, 3, 2, 1],
                index=None,
                format_func=lambda x: f"{x} — {LABELS[x]}",
                label_visibility="collapsed",
            )
    
            st.markdown("**Text clarity / 文本清晰度**")
            text_clarity = st.radio(
                "",
                options=["Clear（清晰）", "Not clear（不清晰）", "No text（无文本）"],
                index=None,
                label_visibility="collapsed",
            )
    
            submitted = st.form_submit_button("Next")

    # ✅ 提交后再校验（这是关键）
    if submitted:
        if score is None or text_clarity is None:
            st.warning("Please select both image quality and text clarity.")
            st.stop()

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
