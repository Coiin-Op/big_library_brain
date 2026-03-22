results = []
import streamlit as st
import subprocess
import pathlib
import os
from pypdf import PdfReader
from ebooklib import epub
from bs4 import BeautifulSoup
from openai import OpenAI

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

st.set_page_config(page_title="Library Brain", page_icon="📚", layout="wide")

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

# --------------------------------------------------
# SIDEBAR (MUST BE FIRST UI)
# --------------------------------------------------

with st.sidebar:

    st.header("Controls")

    max_results = st.slider("Results returned", 10, 100, 30)
    preview_len = st.slider("Preview length", 300, 2000, 1000)

    st.divider()

    st.subheader("Search Result Types")

    pdf_counter = st.empty()
    code_counter = st.empty()
    html_counter = st.empty()
    json_counter = st.empty()
    other_counter = st.empty()

# --------------------------------------------------
# SEARCH FUNCTION
# --------------------------------------------------


def search_recoll(query, max_results=80):

    cmd = [r"D:\recoll\recollq.exe", "-t", "-n", str(max_results), query]

    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore"
    )

    files = []

    for line in result.stdout.splitlines():
        if "file:///" in line:
            try:
                path = line.split("file:///")[1].split("]")[0]
                p = pathlib.Path(path)

                if p.exists():
                    files.append(p)
            except:
                pass

    return list(dict.fromkeys(files))


# --------------------------------------------------
# TEXT EXTRACTION
# --------------------------------------------------


@st.cache_data(show_spinner=False)
def extract_text(path, max_chars=4000):

    try:
        ext = path.suffix.lower()

        if ext == ".pdf":
            reader = PdfReader(str(path))
            text = ""
            for page in reader.pages[:10]:
                text += page.extract_text() or ""
            return text[:max_chars]

        elif ext == ".epub":
            book = epub.read_epub(str(path))
            text = ""
            for item in book.get_items():
                if item.get_type() == 9:
                    soup = BeautifulSoup(item.get_content(), "html.parser")
                    text += soup.get_text()
            return text[:max_chars]

        else:
            return path.read_text(errors="ignore")[:max_chars]

    except:
        return ""


# --------------------------------------------------
# AI FUNCTION
# --------------------------------------------------


def ask_ai(question, context):

    prompt = f"""
Answer the question using the provided context.

Question:
{question}

Context:
{context}

Answer clearly and briefly.
"""

    response = client.chat.completions.create(
        model="qwen2.5-coder-3b-instruct",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    return response.choices[0].message.content


# --------------------------------------------------
# MAIN UI
# --------------------------------------------------

st.title("📚 Library Brain")

query = st.text_input("🔎 Ask your library a question")
# AI SECTION
st.divider()

if results and st.button("🧠 Ask the Library A.I.", key="ask_ai_main"):
    results = []

# --------------------------------------------------
# SEARCH EXECUTION
# --------------------------------------------------

if query:

    results = search_recoll(query, max_results)

    # -----------------------------------------
    # ------------AI Section------------------
    # -----------------------------------------
if results:
    if st.button("🧠 Ask the Library A.I.", key="ask_ai_main"):

        context = ""

        for r in results[:5]:
            text = extract_text(r)
            if text:
                context += f"\n[FILE: {r.name}]\n{text[:2000]}\n"

        try:
            answer = ask_ai(query, context)

            st.subheader("🧠 AI Answer")
            st.write(answer)

        except Exception as e:
            st.error(f"AI error: {str(e)}")
    # ------------------------------------------------

    # COUNT TYPES
    type_counts = {
        "pdf": 0,
        "code": 0,
        "html": 0,
        "json": 0,
        "other": 0,
    }

    for f in results:
        ext = f.suffix.lower()

        if ext == ".pdf":
            type_counts["pdf"] += 1
        elif ext in [".py", ".cpp", ".c", ".js"]:
            type_counts["code"] += 1
        elif ext in [".html", ".htm"]:
            type_counts["html"] += 1
        elif ext == ".json":
            type_counts["json"] += 1
        else:
            type_counts["other"] += 1

    pdf_counter.write(f"📄 PDFs: {type_counts['pdf']}")
    code_counter.write(f"💻 Code: {type_counts['code']}")
    html_counter.write(f"🌐 HTML: {type_counts['html']}")
    json_counter.write(f"🧾 JSON: {type_counts['json']}")
    other_counter.write(f"📁 Other: {type_counts['other']}")

# --------------------------------------------------
# RESULTS DISPLAY
# --------------------------------------------------

if results:

    st.subheader("Top Results")

    for r in results[:10]:

        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            st.write(f"📖 {r.name}")
            st.caption(str(r.parent))

        with col2:
            if st.button("Open", key=f"open_{str(r)}"):
                os.startfile(r)

        with col3:
            if st.button("Preview", key=f"preview_{str(r)}"):
                text = extract_text(r)
                st.text_area(f"Preview: {r.name}", text[:preview_len], height=200)

# --------------------------------------------------
# AI SECTION
# --------------------------------------------------

st.divider()

if results:
    if st.button("🧠 Ask the Library A.I.", key="ask_ai_main"):

        context = ""

        for r in results[:5]:
            text = extract_text(r)
            if text:
                context += f"\n[FILE: {r.name}]\n{text[:2000]}\n"

        try:
            answer = ask_ai(query, context)

            st.subheader("🧠 AI Answer")
            st.write(answer)

        except Exception:
            st.error("AI not connected. Start your model.")
