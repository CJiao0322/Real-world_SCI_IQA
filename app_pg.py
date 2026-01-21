# R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
# USE_R2 = bool(R2_PUBLIC_BASE_URL)

import os
import streamlit as st
# 其他 import …

R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = bool(R2_PUBLIC_BASE_URL)

import streamlit as st
import os
import csv
from datetime import datetime
from uuid import uuid4
from PIL import Image
import random
import time

import psycopg
from psycopg_pool import ConnectionPool
from streamlit_js_eval import streamlit_js_eval

st.set_page_config(layout="wide")

# =========================
# Config
# =========================
import os
DSN = os.environ.get("DATABASE_URL", "postgresql://postgres.sswmzxkgdmzuelhpmvee:skype%408790%21@aws-1-ap-south-1.pooler.supabase.com:6543/postgres?sslmode=require")


# DSN = "postgresql://postgres.sswmzxkgdmzuelhpmvee:skype%408790%21@aws-1-ap-south-1.pooler.supabase.com:6543/postgres?sslmode=require"


DATASET_ROOT = "/Users/ttjiao/capture_all"
MANIFEST_CSV = "manifest_6000.csv"
TRAIN_DIR = "training_images"

LABELS = {
    1: "Bad（差）— 严重失真，如明显模糊、强噪声、文本难以辨认",
    2: "Poor（较差）— 明显失真，细节受损，文本不清晰",
    3: "Fair（一般）— 有一定失真，但仍可接受",
    4: "Good（良好）— 轻微失真，不影响正常观看",
    5: "Excellent（优秀）— 几乎无失真，清晰自然"
}

VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")
TRAIN_INTERVAL_MS = 7000

P = 300
R_TARGET = 25
N_TARGET = 6000
K_PER_PERSON = (N_TARGET * R_TARGET) // P  # 500
COVER_M = 2

# =========================
# PG connection pool
# =========================
# @st.cache_resource
# def get_pool():
#     return ConnectionPool(conninfo=DSN, min_size=1, max_size=20, timeout=30)

# pool = get_pool()

@st.cache_resource
def get_pool():
    return ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=20,
        timeout=30,
        kwargs={
            "prepare_threshold": 0,  # ✅ 禁用 prepared statements，解决 _pg3_0 already exists
        },
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

def get_assigned_image_ids(pid: str):
    rows = pg_exec(
        "SELECT image_id FROM assignments WHERE participant_id=%s ORDER BY ord ASC",
        (pid,),
        fetch=True,
    )
    return [r[0] for r in rows]

def get_image_relpath(image_id: str):
    r = pg_exec("SELECT rel_path FROM images WHERE image_id=%s", (image_id,), fetchone=True)
    return r[0] if r else None

def fetch_distinct_axes():
    cats = [r[0] for r in pg_exec("SELECT DISTINCT category FROM images ORDER BY category", fetch=True)]
    ress = [r[0] for r in pg_exec("SELECT DISTINCT resolution FROM images ORDER BY resolution", fetch=True)]
    dists = [r[0] for r in pg_exec("SELECT DISTINCT distortion FROM images ORDER BY distortion", fetch=True)]
    return cats, ress, dists

def assign_images_for_participant(pid: str):
    # 已分配就不重复
    existing = get_assigned_image_ids(pid)
    if len(existing) >= K_PER_PERSON:
        return

    cats, ress, dists = fetch_distinct_axes()

    # 用事务 + FOR UPDATE SKIP LOCKED 做并发安全分配（关键！）
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")

            chosen = []

            # coverage：每个 strata 取 COVER_M 张
            for cat in cats:
                for res in ress:
                    for dist in dists:
                        cur.execute(
                            """
                            SELECT image_id
                            FROM images
                            WHERE category=%s AND resolution=%s AND distortion=%s AND assigned_count < %s
                            ORDER BY assigned_count ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT %s
                            """,
                            (cat, res, dist, R_TARGET, COVER_M)
                        )
                        rows = cur.fetchall()
                        chosen.extend([r[0] for r in rows])

            # 去重
            seen = set()
            chosen = [x for x in chosen if not (x in seen or seen.add(x))]

            # fill：补齐到 K_PER_PERSON
            need = K_PER_PERSON - len(chosen)
            if need > 0:
                cur.execute(
                    """
                    SELECT image_id
                    FROM images
                    WHERE assigned_count < %s AND NOT (image_id = ANY(%s))
                    ORDER BY assigned_count ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (R_TARGET, chosen, need)
                )
                chosen.extend([r[0] for r in cur.fetchall()])

            random.shuffle(chosen)
            now = datetime.now()

            # 写 assignments
            cur.executemany(
                """
                INSERT INTO assignments (participant_id, image_id, ord, assigned_time)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                [(pid, img_id, i, now) for i, img_id in enumerate(chosen)]
            )

            # 更新 assigned_count（只给本次成功插入的加 1：这里简化为对 chosen 加）
            cur.executemany(
                "UPDATE images SET assigned_count = assigned_count + 1 WHERE image_id=%s",
                [(img_id,) for img_id in chosen]
            )

            conn.commit()

# =========================
# Session
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "intro"
if "participant_id" not in st.session_state:
    st.session_state.participant_id = None
if "idx" not in st.session_state:
    st.session_state.idx = 0

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

    pg_exec(
        "INSERT INTO participants (participant_id, student_id, device, screen_resolution, start_time) VALUES (%s,%s,%s,%s,%s)",
        (pid, student_id.strip(), device, screen_resolution, datetime.now())
    )

    assign_images_for_participant(pid)

    st.session_state.stage = "training"
    st.rerun()


def image_as_data_url(img_path: str, max_side: int, quality: int = 92) -> str:
    """Training 页：编码成 data URL 做轮播（避免 rerun），这里会压缩成 JPEG"""
    with Image.open(img_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"


# def render_training():
#     st.markdown(
#         """
#         <div style="text-align:center; font-weight:950; font-size:30px; margin-top:6px;">
#           Training / 引导示例
#         </div>
#         """,
#         unsafe_allow_html=True,
#     )

#     train_imgs = list_images(TRAIN_DIR)
#     if len(train_imgs) < 5:
#         st.error(f"训练图目录 {TRAIN_DIR}/ 下至少需要 5 张图。当前：{len(train_imgs)}")
#         st.stop()

#     st.info("培训页你现在用的是前端轮播（压缩JPEG），评分页显示原图。培训慢一点可调 TRAIN_INTERVAL_MS。")

#     if st.button("Next → Start Rating"):
#         st.session_state.stage = "rating"
#         st.session_state.idx = 0
#         st.session_state.rating_start_ts = time.time()
#         st.rerun()


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
    if not pid:
        st.error("No participant id.")
        st.stop()

    assigned_ids = get_assigned_image_ids(pid)
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

    # img_path = os.path.join(DATASET_ROOT, rel_path)
    # if not os.path.exists(img_path):
    #     st.error(f"找不到图片：{img_path}")
    #     st.stop()

    if USE_R2:
        img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"
    else:
        img_path = os.path.join(DATASET_ROOT, rel_path)
        if not os.path.exists(img_path):
            st.error(f"找不到图片：{img_path}")
            st.stop()

    

    left, right = st.columns([3.6, 1.4], gap="large")
    # with left:
    #     st.image(img_path, caption=rel_path, use_container_width=True)

    if USE_R2:
        img_url = f"{R2_PUBLIC_BASE_URL}/{rel_path}"
        st.image(img_url, caption=rel_path, use_container_width=True)
    else:
        img_path = os.path.join(DATASET_ROOT, rel_path)
        st.image(img_path, caption=rel_path, use_container_width=True)


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

if st.session_state.stage == "intro":
    render_intro()
elif st.session_state.stage == "training":
    render_training()
elif st.session_state.stage == "rating":
    render_rating()
else:
    render_done()
