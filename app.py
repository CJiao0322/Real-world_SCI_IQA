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

LABELS = {1: "Poor", 2: "Fair", 3: "Good", 4: "Very Good", 5: "Excellent"}
VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")

# ✅ 训练轮播间隔（后台控制，建议 3000~4500）
TRAIN_INTERVAL_MS = 3500

# =========================
# Database
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

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
# Stage 1 — Training (Dock-like 3-up carousel, smooth animation, no rerun)
# =========================
def render_training():
    st.markdown(
        """
        <div style="text-align:center; font-weight:950; font-size:30px; margin-top:6px;">
          Training / 引导示例
        </div>
        <div style="text-align:center; opacity:0.7; font-weight:800; margin-top:6px; margin-bottom:10px;">
          请关注<strong>中间大图</strong>与其底部对应的质量等级。
          建议按 F11 全屏。
        </div>
        """,
        unsafe_allow_html=True,
    )

    train_imgs = list_images(TRAIN_DIR)
    if len(train_imgs) < 5:
        st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图（建议命名：1_poor…5_excellent）。当前：{len(train_imgs)}")
        st.stop()

    train_imgs = train_imgs[:5]  # 排序后前 5 张对应 1~5
    urls = [image_as_data_url(os.path.join(TRAIN_DIR, f), max_side=2600) for f in train_imgs]
    caps = [f"{i+1} — {LABELS[i+1]}" for i in range(5)]

    # 前端轮播（Dock 风格）
    components.html(
        f"""
        <div id="dockWrap" style="width:100%; display:flex; justify-content:center;">
          <div id="dock" style="width:min(1600px, 98vw);">
            <div style="display:flex; align-items:center; justify-content:center; gap:18px;">
              <div class="card small" id="leftCard"><img id="leftImg" /></div>
              <div class="card big" id="centerCard"><img id="centerImg" /></div>
              <div class="card small" id="rightCard"><img id="rightImg" /></div>
            </div>

            <div style="margin-top:12px; text-align:center;">
              <div id="centerCap" style="font-weight:950; font-size:22px;"></div>
              <div style="opacity:0.65; font-weight:800; font-size:13px; margin-top:4px;">
                Focus on the center image. (No scores recorded on this page)
              </div>
            </div>
          </div>
        </div>

        <style>
          #dock .card {{
            border-radius: 18px;
            overflow: hidden;
            background: #fff;
            border: 1px solid rgba(0,0,0,0.10);
            will-change: transform, opacity;
          }}
          #dock img {{
            width: 100%;
            height: auto;
            display: block;
            background: #fff;
          }}

          /* Dock 风格：中间突出 */
          #dock .small {{
            flex: 1.05;
            opacity: 0.45;
            transform: translateX(0px) scale(0.90);
            filter: saturate(0.95);
            box-shadow: 0 8px 18px rgba(0,0,0,0.08);
            transition: transform 420ms ease, opacity 420ms ease, filter 420ms ease;
          }}
          #dock .big {{
            flex: 2.90;
            opacity: 1.0;
            transform: translateX(0px) scale(1.0);
            box-shadow: 0 18px 40px rgba(0,0,0,0.16);
            border: 2px solid rgba(17,24,39,0.9);
            transition: transform 420ms ease, opacity 420ms ease;
          }}

          /* “滑动感”关键：轮播时给整体一个轻微左移，然后再换图 */
          #dock.moving .small {{
            transform: translateX(-10px) scale(0.90);
            opacity: 0.35;
          }}
          #dock.moving .big {{
            transform: translateX(-14px) scale(1.0);
          }}

          /* 让中间图更吸引注意：淡淡光晕 */
          #centerCard {{
            box-shadow: 0 18px 44px rgba(0,0,0,0.18), 0 0 0 6px rgba(17,24,39,0.06);
          }}
        </style>

        <script>
          const urls = {urls};
          const caps = {caps};
          const interval = {TRAIN_INTERVAL_MS};

          const leftImg = document.getElementById("leftImg");
          const centerImg = document.getElementById("centerImg");
          const rightImg = document.getElementById("rightImg");
          const centerCap = document.getElementById("centerCap");
          const dock = document.getElementById("dock");

          // center index
          let c = 0;

          function setTriplet() {{
            const l = (c + urls.length - 1) % urls.length;
            const r = (c + 1) % urls.length;

            leftImg.src = urls[l];
            centerImg.src = urls[c];
            rightImg.src = urls[r];

            centerCap.textContent = "CENTER: " + caps[c];
          }}

          // 初始
          setTriplet();

          function advance() {{
            // 先做一个“滑动/轮动”的视觉提示
            dock.classList.add("moving");

            // 过一小段时间再真正换图，形成“右滑入→居中”的感知
            setTimeout(() => {{
              c = (c + 1) % urls.length;   // 右 -> 中 -> 左：中心前进
              setTriplet();
              dock.classList.remove("moving");
            }}, 380);
          }}

          // 稍慢一点，避免一上来就切换
          setTimeout(() => {{
            setInterval(advance, interval);
          }}, 900);
        </script>
        """,
        height=720,
    )

    # Next 按钮放下面（Streamlit 逻辑不变）
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    if st.button("Next → Start Rating"):
        st.session_state.stage = "rating"
        st.session_state.idx = 0
        st.rerun()

# =========================
# Stage 2 — Rating
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
        next_clicked = st.button("Next", disabled=(score is None))

    if next_clicked:
        conn = get_conn()
        conn.execute(
            "INSERT INTO ratings VALUES (?, ?, ?, ?, ?)",
            (
                st.session_state.participant_id,
                img_name,
                int(score),
                LABELS[int(score)],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
