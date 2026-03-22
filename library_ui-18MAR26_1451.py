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


def score_text(text, query):
    score = 0
    q_words = query.lower().split()
    text_lower = text.lower()

    for word in q_words:
        score += text_lower.count(word)

    return score


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


def chunk_text(text, size=800, overlap=200):
    chunks = []
    start = 0

    while start < len(text):
        end = start + size
        chunk = text[start:end]
        chunks.append(chunk)
        start += size - overlap

    return chunks


def refine_query(original_question, context):

    prompt = f"""
You are helping refine a research query.

Original question:
{original_question}

Based on partial context below, generate a better search query
that would retrieve more relevant technical information.

Context:
{context[:1000]}

Return ONLY the improved search query.
"""

    response = client.chat.completions.create(
        model="mixtral-8x7b-instruct-v0.1",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    return response.choices[0].message.content.strip()


# ----------------------------------------------------

st.title("Library Brain")

query = st.text_input("Ask your library a question")

st.divider()

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
Answer ONLY using the provided context.
If the answer is not in the context, say "Not found in library."

Question:
{question}

Context:
{context}

Answer clearly and briefly.
"""

    response = client.chat.completions.create(
        model="mixtral-8x7b-instruct-v0.1",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )

    return response.choices[0].message.content


# --------------------------------------------------
# MAIN UI
# --------------------------------------------------

if query:

    results = search_recoll(query, max_results)

    # AI BUTTON
    if results and st.button("Ask the Library AI", key="ask_ai_main"):

        try:
            context = ""

            for r in results:
                text = extract_text(r)
                if not text:
                    continue

                chunks = chunk_text(text)

                scored_chunks = []

            for chunk in chunks:
                score = chunk.lower().count(query.lower())
                if score > 2:
                    scored_chunks.append((score, chunk))

                scored_chunks.sort(reverse=True, key=lambda x: x[0])

                for score, chunk in scored_chunks[:3]:
                    context += f"\nSOURCE: {r.name}\n{chunk}\n"

                if len(context) > 4000:
                    break

            raw = ask_ai(query, context)

            st.subheader("AI Answer")
            st.write(raw)

        except Exception as e:
            st.error(f"AI error: {str(e)}")

    # SOURCES
    st.subheader("Sources Used")
    for r in results[:5]:
        st.write(r.name)

    # FILE TYPE COUNTS
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

    pdf_counter.write(f"PDFs: {type_counts['pdf']}")
    code_counter.write(f"Code: {type_counts['code']}")
    html_counter.write(f"HTML: {type_counts['html']}")
    json_counter.write(f"JSON: {type_counts['json']}")
    other_counter.write(f"Other: {type_counts['other']}")

    # RESULTS LIST
    st.subheader("Top Results")

    for r in results[:10]:

        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            st.write(r.name)
            st.caption(str(r.parent))

        with col2:
            if st.button("Open", key=f"open_{str(r)}"):
                os.startfile(r)

        with col3:
            if st.button("Preview", key=f"preview_{str(r)}"):
                text = extract_text(r)
                st.text_area(f"Preview: {r.name}", text[:preview_len], height=200)
