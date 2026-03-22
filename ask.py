import pathlib
import subprocess

from pypdf import PdfReader
from ebooklib import epub
from bs4 import BeautifulSoup

# -----------------------------
# TEXT EXTRACTION
# -----------------------------


def read_txt(path):
    try:
        return path.read_text(errors="ignore")
    except:
        return ""


def read_html(path):
    try:
        text = path.read_text(errors="ignore")
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text()
    except:
        return ""


def read_pdf(path):
    try:
        reader = PdfReader(str(path))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    except:
        return ""


def read_epub(path):
    try:
        book = epub.read_epub(str(path))
        text = ""

        for item in book.get_items():
            if item.get_type() == 9:
                soup = BeautifulSoup(item.get_content(), "html.parser")
                text += soup.get_text()

        return text
    except:
        return ""


def extract_text(path):

    ext = path.suffix.lower()

    if ext in [".txt", ".py", ".md", ".json"]:
        return read_txt(path)

    if ext == ".pdf":
        return read_pdf(path)

    if ext == ".epub":
        return read_epub(path)

    if ext in [".html", ".htm"]:
        return read_html(path)

    return ""


def search_recoll(query, max_results=50):

    cmd = [r"D:\recoll\recollq.exe", "-t", "-n", str(max_results), query]

    result = subprocess.run(cmd, capture_output=True, text=True)

    files = []

    for line in result.stdout.splitlines():

        line = line.strip()

        if not line:
            continue

        parts = line.split()

        path = parts[-1]  # last column is usually the path

        p = pathlib.Path(path)

        if p.exists():
            files.append(p)

    return files


# -----------------------------
# MAIN LOOP
# -----------------------------

while True:

    query = input("\nAsk something: ").strip()

    if not query:
        continue

    print("\n--- RECOLL RESULTS ---\n")

    results = search_recoll(query)

    for r in results[:10]:
        print(r)

    print("\n--- BUILDING CONTEXT ---\n")

    context = ""

    readable_ext = [".txt", ".py", ".md", ".json", ".pdf", ".epub", ".html", ".htm"]

    count = 0

    for path in results:

        if path.suffix.lower() not in readable_ext:
            continue

        text = extract_text(path)

        if not text:
            continue

        snippet = text[:3000]

        context += f"\n[FILE: {path.name}]\n{snippet}\n"

        count += 1

        if count >= 5:
            break

    print(context[:4000])

    print("\n--- ANSWER ---\n")
