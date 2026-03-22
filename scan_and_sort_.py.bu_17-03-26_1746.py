# file: book_sorter.py

import os
import shutil
import sqlite3
import time
import subprocess
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pypdf import PdfReader
from openai import OpenAI

SOURCE_DIR = r"Z:\unsorted_books"
DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"
MAX_WORKERS = 4

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")


# --- DB ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            path TEXT PRIMARY KEY,
            category TEXT
        )
    """)
    conn.commit()
    conn.close()


def get_cached(conn, path):
    c = conn.cursor()
    c.execute("SELECT category FROM cache WHERE path=?", (path,))
    row = c.fetchone()
    return row[0] if row else None


def save_cache(conn, path, category):
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (path, category))
    conn.commit()


# --- TEXT EXTRACTION ---
def extract_preview(path, max_chars=1200):
    try:
        if path.lower().endswith(".pdf"):
            reader = PdfReader(path)
            text = ""
            for page in reader.pages[:2]:
                text += page.extract_text() or ""
            return text[:max_chars]
        else:
            with open(path, "r", errors="ignore") as f:
                return f.read(max_chars)
    except:
        return ""


# --- RECOLL ---
def get_recoll_files(query="*"):
    cmd = [r"D:\recoll\recollq.exe", "-t", "-n", "1000", query]

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
                    files.append(str(p))
            except:
                pass

    return list(dict.fromkeys(files))


# --- LLM ---
def classify_book(name, text, retries=3):
    prompt = f"""
Classify into ONE category:

Fiction:
Sci-Fi, Fantasy, Mystery, Horror, Romance, Other Fiction

Non-Fiction:
Science, Engineering, Medical, History, Philosophy, Other Non-Fiction

Book: {name}
Preview: {text[:1000]}

Answer ONLY category.
"""

    for _ in range(retries):
        try:
            res = client.chat.completions.create(
                model="mixtral",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            return res.choices[0].message.content.strip()
        except:
            time.sleep(1)

    return "Unknown"


# --- WORKER ---
def process_file(path):
    conn = sqlite3.connect(DB_PATH)

    try:
        if not os.path.isfile(path):
            return

        cached = get_cached(conn, path)
        if cached:
            print(f"[CACHE] {os.path.basename(path)} → {cached}")
            return

        print(f"\nProcessing: {os.path.basename(path)}")

        preview = extract_preview(path)
        if not preview.strip():
            print("→ Skipped (no text)")
            return

        category = classify_book(os.path.basename(path), preview)

        save_cache(conn, path, category)

        print(f"→ {category}")
        print(f"WOULD MOVE → {path} → {category}")

    finally:
        conn.close()


# --- MAIN ---
def process_recoll():
    init_db()

    files = get_recoll_files("ext:pdf")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, f) for f in files]

        for _ in as_completed(futures):
            pass


if __name__ == "__main__":
    process_recoll()