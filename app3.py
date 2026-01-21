import streamlit as st
import os
import sqlite3
from datetime import datetime
from uuid import uuid4
from PIL import Image
import io
import base64
import streamlit.components.v1 as components

st.set_page_config(layout="wide")

# =========================
# Config
# =========================
IMG_DIR = "images"
TRAIN_DIR = "training_images"
DB_PATH = "results.db"

LABELS = {1: "Bad", 2: "Poor", 3: "Fair", 4: "Good", 5: "Excellent"}
VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")

# ✅ 训练轮播间隔（后台控制，建议 3000~4500）
TRAIN_INTERVAL_MS = 3500

# =========================
# Database
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_ratings_columns(conn):
    """
    ✅ 自动迁移：如果旧库没有 text_clarity / color_correctness 两列，就自动加上
    不用你手动删 results.db
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(ratings)")
    cols = {row[1] for row in cur.fetchall()}  # row[1] = column name

    if "text_clarity" not in cols:
        cur.execute("ALTER TABLE ratings ADD COLUMN text_clarity TEXT")
    if "color_correctness" not in cols:
        cur.execute("ALTER TABLE ratings ADD COLUMN color_correctness TEXT")

    conn.commit()

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        student_id TEXT,
        device TEXT,
        resolution TEXT,
        start_time TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        participant_id TEXT,
        image_name TEXT,
        score INTEGER,
        label TEXT,
        time TEXT
    )
    """)
    conn.commit()

    # ✅ 自动补齐新列
    ensure_ratings_columns(conn)

    conn.close()

init_db()

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
    """
    Training 页：把图编码成 data URL 给前端 JS 做顺滑轮播（不 rerun）。
    max_side 越大越清晰，但也越重；训练页建议 2200~3000。
    """
    with Image.open(img_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"

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
# CSS (full width + next button)
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
        resolution = st.selectbox(
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
            (pid, student_id.strip(), device, resolution, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()

        st.session_state.stage = "training"
        st.rerun()

# =========================
# Stage 1 — Training (single image, no rerun)
# =========================
def render_training():
    st.markdown(
        """
        <div style="text-align:center; font-weight:950; font-size:30px; margin-top:6px;">
          Training / 引导示例
        </div>
        <div style="text-align:center; opacity:0.7; font-weight:800; margin-top:6px; margin-bottom:10px;">
          请观察图像失真与对应质量等级（一次只呈现一张）。
        </div>
        """,
        unsafe_allow_html=True,
    )

    train_imgs = list_images(TRAIN_DIR)
    if len(train_imgs) < 5:
        st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图（建议命名：1_poor…5_excellent）。当前：{len(train_imgs)}")
        st.stop()

    train_imgs = train_imgs[:5]  # 排序后前 5 张对应 1~5

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
# Stage 2 — Rating (+ 2 extra questions)
# =========================
def render_rating():
    images = list_images(IMG_DIR)
    if len(images) == 0:
        st.error(f"{IMG_DIR}/ 目录下没有可评分图像。")
        st.stop()

    if st.session_state.idx >= len(images):
        st.session_state.stage = "done"
        st.rerun()
        return

    img_name = images[st.session_state.idx]
    img_path = os.path.join(IMG_DIR, img_name)

    left, right = st.columns([3.6, 1.4], gap="large")
    with left:
        st.image(img_path, caption=img_name, use_container_width=True)

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
            INSERT INTO ratings (participant_id, image_name, score, label, time, text_clarity, color_correctness)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                st.session_state.participant_id,
                img_name,
                int(score),
                LABELS[int(score)],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                str(text_clarity),
                str(color_correctness),
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
