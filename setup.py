import streamlit as st
import pandas as pd
import os
from datetime import datetime
from PIL import Image
import streamlit.components.v1 as components

st.set_page_config(layout="wide")

IMG_DIR = "images"
SAVE_PATH = "scores.xlsx"
LABELS = ["Poor", "Fair", "Good", "Very Good", "Excellent"]

# ---- init ----
# img_list = sorted(os.listdir(IMG_DIR))
VALID_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")

img_list = sorted(
    f for f in os.listdir(IMG_DIR)
    if f.lower().endswith(VALID_EXTS)
)


if "idx" not in st.session_state:
    st.session_state.idx = 0
if "score" not in st.session_state:
    st.session_state.score = 3  # default

if st.session_state.idx >= len(img_list):
    st.success("所有图像已评分完成！")
    st.stop()

# ---- CSS ----
st.markdown(
    """
    <style>
    .block-container{
        padding-left: 1.2rem;
        padding-right: 1.2rem;
        max-width: 1800px;
        margin: 0 auto;
    }
    .img-wrap img{
        width: 100% !important;
        height: auto !important;
        max-height: 90vh !important;
        object-fit: contain !important;
        display: block;
        margin: 0 auto;
    }
    .panel{
        position: sticky;
        top: 1rem;
        border: 1px solid rgba(0,0,0,0.08);
        border-radius: 14px;
        padding: 14px 14px 12px 14px;
        background: white;
        box-shadow: 0 6px 20px rgba(0,0,0,0.06);
    }
    div.stButton > button{
        width: 100%;
        padding: 0.65rem 0.9rem;
        border-radius: 12px;
        font-weight: 700;
    }
    .lab{
        font-size: 14px;
        font-weight: 700;
        opacity: 0.65;
        line-height: 1.15;
        white-space: nowrap;
    }
    .lab.active{
        opacity: 1.0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---- current image ----
img_name = img_list[st.session_state.idx]
img_path = os.path.join(IMG_DIR, img_name)
image = Image.open(img_path)

left, right = st.columns([3.6, 1.4], gap="large")

with left:
    st.markdown('<div class="img-wrap">', unsafe_allow_html=True)
    st.image(image, caption=img_name, width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Rate image quality")
    st.caption("Drag the red knob up/down (5 = Excellent)")

    # ---- TRUE vertical slider component ----
    # It posts value back to Streamlit via query param style message, captured by components.html return.
    slider_html = f"""
    <div style="display:flex; gap:18px; align-items:center; justify-content:center; padding:8px 0 4px 0;">
      <div style="height:260px; display:flex; align-items:center; justify-content:center;">
        <input id="v" type="range" min="1" max="5" step="1" value="{st.session_state.score}"
          style="
            -webkit-appearance: slider-vertical;
            appearance: slider-vertical;
            writing-mode: bt-lr;
            height: 260px;
            width: 28px;
            accent-color: #e74c3c;
          "
          oninput="document.getElementById('val').innerText=this.value;"
          onchange="parent.postMessage({{type:'vslider', value:this.value}}, '*');"
        />
      </div>

      <div style="height:260px; display:flex; flex-direction:column; justify-content:space-between;">
        <div style="font-weight:800; opacity:{1.0 if st.session_state.score==5 else 0.55};">5 — Excellent</div>
        <div style="font-weight:800; opacity:{1.0 if st.session_state.score==4 else 0.55};">4 — Very Good</div>
        <div style="font-weight:800; opacity:{1.0 if st.session_state.score==3 else 0.55};">3 — Good</div>
        <div style="font-weight:800; opacity:{1.0 if st.session_state.score==2 else 0.55};">2 — Fair</div>
        <div style="font-weight:800; opacity:{1.0 if st.session_state.score==1 else 0.55};">1 — Poor</div>
      </div>
    </div>

    <div style="text-align:center; font-weight:700; margin-top:6px;">
      Selected: <span id="val">{st.session_state.score}</span>
    </div>

    <script>
      // Listen messages back from parent (Streamlit)
      window.addEventListener('message', (event) => {{
        // no-op
      }});
    </script>
    """
    # capture posted message in Streamlit (hack: use components + JS postMessage + Streamlit event)
    # Streamlit doesn't directly expose postMessage to python, so we use a small trick:
    # we re-render and store score via a hidden query string using setComponentValue (Streamlit components protocol)
    slider_component = components.html(
        f"""
        <script>
        const send = (v) => {{
          const data = {{isStreamlitMessage: true, type: "streamlit:setComponentValue", value: v}};
          window.parent.postMessage(data, "*");
        }};
        window.addEventListener("message", (event) => {{
          if(event.data && event.data.type === 'vslider') {{
            send(event.data.value);
          }}
        }});
        </script>
        {slider_html}
        """,
        height=330,
    )

    # components.html will return the last setComponentValue -> slider_component
    # It may come back as string
    if slider_component is not None:
        try:
            st.session_state.score = int(slider_component)
        except:
            pass

    st.markdown(f"**Your rating:** {st.session_state.score} — *{LABELS[st.session_state.score-1]}*")

    next_clicked = st.button("Next")
    st.markdown("</div>", unsafe_allow_html=True)

# ---- save / next ----
if next_clicked:
    score = st.session_state.score
    row = {
        "image": img_name,
        "score": score,
        "label": LABELS[score - 1],
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    if os.path.exists(SAVE_PATH):
        df = pd.read_excel(SAVE_PATH)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_excel(SAVE_PATH, index=False)
    st.session_state.idx += 1
    st.rerun()
