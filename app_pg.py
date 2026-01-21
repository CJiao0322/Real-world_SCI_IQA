import os
import time
import random
import csv
from datetime import datetime
from uuid import uuid4

import streamlit as st
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval

import psycopg
from psycopg_pool import ConnectionPool


st.set_page_config(layout="wide")


# =========================
# Config
# =========================
DSN = os.environ.get("DATABASE_URL", "").strip()
if not DSN:
    st.error("Missing env var: DATABASE_URL")
    st.stop()

R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").strip().rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)

TRAIN_DIR = "training_images"
TRAIN_INTERVAL_MS = 7000

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然"
}


# =========================
# PG Pool (fast + safe)
# =========================
@st.cache_resource
def get_pool():
    # 注意：psycopg3 的 prepared statement 问题，我们不用任何 prepare_threshold 参数
    return ConnectionPool(conninfo=DSN, min_size=1, max_size=20, timeout=30)

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
# DB helpers (slot-based)
# =========================
@st.cache_data(ttl=60)
def get_exp_config():
    r = pg_exec(
        "SELECT p_total, r_target, n_images, k_per_person FROM exp_config WHERE id=1",
        fetchone=True
    )
    if not r:
        return None
    return {"P": int(r[0]), "R": int(r[1]), "N": int(r[2]), "K": int(r[3])}


def get_next_slot_and_increment():
    """
    原子发号：slot_counter.next_slot 从 1 开始，取完 +1
    如果超过 P，就循环回 1（你也可以改成“超过就停止”）
    """
    cfg = get_exp_config()
    if not cfg:
        raise RuntimeError("exp_config not found, please run make_assignment_plan_from_manifest.py")

    P = cfg["P"]

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            # 锁住这行，保证并发安全
            cur.execute("SELECT next_slot FROM slot_counter WHERE id=1 FOR UPDATE")
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO slot_counter (id, next_slot) VALUES (1, 1)")
                next_slot = 1
            else:
                next_slot = int(row[0])

            slot = next_slot
            next_slot = next_slot + 1
            if next_slot > P:
                next_slot = 1

            cur.execute("UPDATE slot_counter SET next_slot=%s WHERE id=1", (next_slot,))
            conn.commit()

    return slot


def get_plan_image_ids_by_slot(slot: int):
    rows = pg_exec(
        "SELECT image_id FROM assignment_plan WHERE slot=%s ORDER BY ord ASC",
        (slot,),
        fetch=True
    )
    return [r[0] for r in rows]


@st.cache_data(ttl=3600)
def get_image_relpath(image_id: str):
    r = pg_exec("SELECT rel_path FROM images WHERE image_id=%s", (image_id,), fetchone=True)
    return r[0] if r else None


# =========================
# UI helpers
# =========================
@st.cache_data(show_spinner=False)
def list_images(folder):
    exts = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")
    if not os.path.exists(folder):
        return []
    return sorted([f for f in os.listdir(folder) if f.lower().endswith(exts) and not f.startswith(".")])


@st.cache_data(show_spinner=False)
def image_as_data_url(img_path: str, max_side: int, quality: int = 88) -> str:
    # training 用，尽量快
    from PIL import Image
    import io, base64

    with Image.open(img_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"


def img_url_from_relpath(rel_path: str) -> str:
    # rel_path 里面可能有空格/特殊字符，保险起见 encode
    from urllib.parse import quote
    return f"{R2_PUBLIC_BASE_URL}/{quote(rel_path)}"


# =========================
# Session State
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "slot" not in st.session_state:
    st.session_state.slot = None
if "assigned_ids" not in st.session_state:
    st.session_state.assigned_ids = None
if "idx" not in st.session_state:
    st.session_state.idx = 0


# =========================
# Pages
# =========================
def render_intro():
    st.title("Image Quality Assessment Experiment")

    cfg = get_exp_config()
    if cfg:
        st.caption(f"Experiment config: N={cfg['N']} images · P={cfg['P']} slots · K={cfg['K']} per person · R={cfg['R']}")

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

    # 1) 发一个 slot（并发安全）
    slot = get_next_slot_and_increment()

    # 2) participant_id
    pid = str(uuid4())

    # 3) 写 participants
    pg_exec(
        """
        INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (pid, student_id.strip(), device, screen_resolution, datetime.now())
    )

    # 4) 读取该 slot 的图片列表（一次拿完，存在 session，后续不再查计划表）
    assigned_ids = get_plan_image_ids_by_slot(slot)
    if not assigned_ids:
        st.error(f"No images found for slot={slot}. Please check assignment_plan import.")
        return

    # 5) 写 assignments（可选，但建议保留：方便你后续查某人拿了哪些图）
    now = datetime.now()
    rows = [(pid, img_id, i, now) for i, img_id in enumerate(assigned_ids)]
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                rows
            )
        conn.commit()

    st.session_state.participant_id = pid
    st.session_state.slot = slot
    st.session_state.assigned_ids = assigned_ids
    st.session_state.idx = 0
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

    train_imgs = list_images(TRAIN_DIR)
    if len(train_imgs) < 5:
        st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图。当前：{len(train_imgs)}")
        st.stop()

    # ✅ 确定性：固定取前 5 张（按文件名排序）
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


def render_rating():
    pid = st.session_state.participant_id
    assigned_ids = st.session_state.assigned_ids

    if not pid or not assigned_ids:
        st.error("Session lost. Please restart from the homepage.")
        st.stop()

    total = len(assigned_ids)
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

    image_id = assigned_ids[done]
    rel_path = get_image_relpath(image_id)
    if not rel_path:
        st.error("Image not found in DB.")
        st.stop()

    left, right = st.columns([3.6, 1.4], gap="large")

    with left:
        if USE_R2:
            st.image(img_url_from_relpath(rel_path), caption=rel_path, use_container_width=True)
        else:
            st.error("R2_PUBLIC_BASE_URL not set. Please set it in Render env.")
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

        st.markdown("---")
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
else:
    render_done()
