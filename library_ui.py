import streamlit as st
import subprocess
import pathlib
import os
import hashlib
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
# SIDEBAR
# --------------------------------------------------

with st.sidebar:

    st.header("Controls")

    library_scope = st.radio(
        "Search in:",
        [
            "All Libraries",
            "Research Library",
            "Reference Library",
            "Fiction Library",
            "Manuals",
            "Science Mode",
            "Silent Death Project",
        ],
    )

    max_results = st.slider("Results returned", 10, 100, 30)
    preview_len = st.slider("Preview length", 300, 2000, 1000)

    filetype = st.selectbox("File type", ["all", "pdf", "epub", "html", "txt", "doc"])

    st.divider()
    st.subheader("Search Result Types")

    pdf_counter = st.empty()
    code_counter = st.empty()
    html_counter = st.empty()
    json_counter = st.empty()
    other_counter = st.empty()

# --------------------------------------------------
# HELPERS
# --------------------------------------------------


def chunk_text(text, size=800, overlap=200):
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end])
        start += size - overlap
    return chunks


CACHE_DIR = pathlib.Path("D:/recoll/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(file_path):
    key = hashlib.md5(file_path.encode()).hexdigest()
    return CACHE_DIR / f"{key}.txt"


@st.cache_data(show_spinner=False)
def extract_text(path, max_chars=4000):

    cache_file = get_cache_path(str(path))

    if cache_file.exists():
        return cache_file.read_text(errors="ignore")[:max_chars]

    try:
        ext = path.suffix.lower()

        if ext == ".pdf":
            reader = PdfReader(str(path))
            text = ""
            for page in reader.pages[:10]:
                text += page.extract_text() or ""

        elif ext == ".epub":
            book = epub.read_epub(str(path))
            text = ""
            for item in book.get_items():
                if item.get_type() == 9:
                    soup = BeautifulSoup(item.get_content(), "html.parser")
                    text += soup.get_text()

        else:
            text = path.read_text(errors="ignore")

        cache_file.write_text(text, errors="ignore")
        return text[:max_chars]

    except:
        return ""


def search_recoll(query, max_results=20):

    cmd = [r"D:\recoll\recollq.exe", "-n", str(max_results), query]

    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore"
    )

    entries = []

    for line in result.stdout.splitlines():
        line = line.strip()

        if "file:///" in line:
            try:
                start = line.find("file:///") + 8
                end = line.find("]", start)
                path = line[start:end]
                path = path.replace("/", "\\")

                entries.append({"path": path, "abstract": line})
            except:
                continue

    return entries


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

st.title("Library Brain")
query = st.text_input("Ask your library a question")

if query:

    results = search_recoll(query, max_results)

    # ---------- FILETYPE FILTER ----------
    if filetype != "all":
        results = [
            r for r in results if r.get("path", "").lower().endswith(f".{filetype}")
        ]

    # ---------- COUNTS ----------
    type_counts = {"pdf": 0, "code": 0, "html": 0, "json": 0, "other": 0}

    for r in results:
        p = r.get("path", "").lower()

        if p.endswith(".pdf"):
            type_counts["pdf"] += 1
        elif p.endswith((".py", ".cpp", ".c", ".js", ".cs", ".java", ".txt")):
            type_counts["code"] += 1
        elif p.endswith((".html", ".htm", ".xml", ".epub")):
            type_counts["html"] += 1
        elif p.endswith(".json"):
            type_counts["json"] += 1
        else:
            type_counts["other"] += 1

    pdf_counter.metric("PDFs", type_counts["pdf"])
    code_counter.metric("Code", type_counts["code"])
    html_counter.metric("HTML", type_counts["html"])
    json_counter.metric("JSON", type_counts["json"])
    other_counter.metric("Other", type_counts["other"])

    st.write("DEBUG RESULTS:", len(results))

    # ---------- AI ----------
    if results and st.button("Ask the Library AI"):

        context = ""
        MAX_CONTEXT = 3000

        for r in results[:3]:
            path = r.get("path", "")
            brain_file = pathlib.Path(path).with_suffix(".brain.json")

            if brain_file.exists():
                import json

                data = json.loads(brain_file.read_text(errors="ignore"))
                text = data.get("snippet", "")
            else:
                text = extract_text(pathlib.Path(path))

            if not text:
                continue

            chunks = chunk_text(text)

            scored = []

            for chunk in chunks:
                score = 0

                for w in query.lower().split():
                    if w in chunk.lower():
                        score += 2

                # apply bonus OUTSIDE word loop
                if "category" in str(path).lower():
                    score += 3

                scored.append((score, chunk))  # always append

            scored.sort(reverse=True)

            for _, chunk in scored[:1]:
                block = f"\nSOURCE: {path}\n{chunk}\n"

                if len(context) + len(block) < MAX_CONTEXT:
                    context += block
                else:
                    break

        answer = ask_ai(query, context)

        st.subheader("AI Answer")
        st.write(answer)

    # ---------- RESULTS ----------
    st.subheader("Top Results")

    if not results:
        st.warning("No results found.")
    else:
        for r in results[:10]:

            path = r.get("path", "unknown")

            col1, col2, col3 = st.columns([4, 1, 1])

            with col1:
                st.write(path)

            with col2:
                if st.button("Open", key=f"open_{path}"):
                    try:
                        os.startfile(path)
                    except:
                        st.error("Could not open file")

            with col3:
                if st.button("Preview", key=f"preview_{path}"):
                    try:
                        text = extract_text(pathlib.Path(path))
                        st.text_area("Preview", text[:preview_len], height=200)
                    except:
                        st.error("Preview failed")
