import os
import time
import logging
from uuid import uuid4
from datetime import datetime

import streamlit as st
import psycopg
from psycopg_pool import ConnectionPool
import streamlit.components.v1 as components


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
    st.error("R2_PUBLIC_BASE_URL not set. This app expects images loaded from R2.")
    st.stop()

P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P  # 500
COVER_M = 2

TRAIN_INTERVAL_MS = 7000  # ✅ keep yours

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然"
}

# =========================
# Pool  (Render + Supabase pooler 更稳)
# =========================
@st.cache_resource
def get_pool():
    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=3,
        timeout=60,
    )

pool = get_pool()

# =========================
# Connection init per-use
# =========================
INIT_SQL = """
SET statement_timeout = '30s';
SET idle_in_transaction_session_timeout = '30s';
SET plan_cache_mode = force_generic_plan;
"""

def _init_conn(conn):
    try:
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def pg_exec(sql, params=None, fetch=False, fetchone=False):
    t0 = time.time()
    with pool.connection(timeout=60) as conn:
        _init_conn(conn)

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
# Helpers
# =========================
def r2_url(rel_path: str) -> str:
    return f"{R2_PUBLIC_BASE_URL}/{rel_path.lstrip('/')}"

@st.cache_data(show_spinner=False, ttl=600)
def get_training_relpaths(limit=5):
    """
    ✅ 不依赖本地 TRAIN_DIR
    直接从 images 表拿 5 张做 training（非常快：按 image_id 排序 + LIMIT）
    """
    rows = pg_exec(
        "SELECT rel_path FROM images ORDER BY image_id ASC LIMIT %s",
        (limit,),
        fetch=True,
    )
    return [r[0] for r in rows] if rows else []

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
if "training_urls" not in st.session_state:
    st.session_state.training_urls = None

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

    # quick connection test
    try:
        pg_exec("SELECT 1", fetchone=True)
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        st.stop()

    # insert participant
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

    # ✅ training samples (from DB -> R2 URL)
    train_rel = get_training_relpaths(5)
    st.session_state.training_urls = [r2_url(p) for p in train_rel]

    st.session_state.stage = "training"  # ✅ intro -> training
    st.session_state.idx = 0
    st.session_state.rating_start_ts = None
    st.rerun()

# ✅ 你给的 training 函数（稍微改动：urls 直接用 R2，不用本地 list_images / image_as_data_url）
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

    urls = st.session_state.training_urls or []
    if len(urls) < 5:
        # fallback：临时再取一次
        train_rel = get_training_relpaths(5)
        urls = [r2_url(p) for p in train_rel]
        st.session_state.training_urls = urls

    if len(urls) < 5:
        st.error("Training images not available (need at least 5). Check images table / R2 paths.")
        st.stop()

    urls = urls[:5]
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

    if st.session_state.rating_start_ts is None:
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

    image_id, rel_path = pairs[done]

    left, right = st.columns([3.6, 1.4], gap="large")
    with left:
        st.image(r2_url(rel_path), caption=rel_path, use_container_width=True)

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
elif st.session_state.stage == "training":
    render_training()
elif st.session_state.stage == "rating":
    render_rating()
else:
    render_done()
