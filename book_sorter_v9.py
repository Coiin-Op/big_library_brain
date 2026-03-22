import os
import shutil
import sqlite3
import time
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from openai import OpenAI

# --- CONFIG ---
SOURCE_DIRS = [r"Z:\organized_books\00_Unknown"]

DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"

MAX_WORKERS = 4  # keep system usable
BATCH_SIZE = 5  # HUGE speed gain here

MODEL_NAME = "mistral-7b-instruct-v0.3.Q4_K_M.gguf"

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

# --- GLOBALS ---
lock = Lock()
processed = 0
start_time = time.time()

# --- FILE FILTER ---
ALLOWED = {
    ".pdf",
    ".txt",
    ".epub",
    ".mobi",
    ".azw",
    ".azw3",
    ".lit",
    ".fb2",
    ".html",
    ".htm",
    ".xml",
    ".rtf",
    ".doc",
    ".docx",
    ".chm",
}

BLOCKED = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".jpg",
    ".png",
    ".bmp",
    ".gif",
    ".exe",
    ".dll",
    ".bin",
    ".iso",
}


def valid(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in BLOCKED:
        return False
    if ext not in ALLOWED:
        return False
    try:
        return os.path.getsize(path) > 2000
    except:
        return False


# --- DB ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache (
            path TEXT PRIMARY KEY,
            category TEXT,
            hash TEXT
        )
    """
    )
    conn.commit()
    conn.close()


def cached(conn, path):
    return conn.execute("SELECT 1 FROM cache WHERE path=?", (path,)).fetchone()


def save(conn, path, cat, h):
    conn.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?)", (path, cat, h))
    conn.commit()


def dup(conn, h):
    return conn.execute("SELECT category FROM cache WHERE hash=?", (h,)).fetchone()


# --- HASH ---
def file_hash(path):
    try:
        with open(path, "rb") as f:
            return hashlib.md5(f.read(4096)).hexdigest()
    except:
        return None


# --- CLEAN ---
def clean(text):
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    return text.strip()


# --- PREVIEW ---
def preview(path):
    try:
        with open(path, "r", errors="ignore") as f:
            return clean(f.read(250))
    except:
        return ""


# --- QUICK CLASSIFIER ---
def quick(name):
    n = name.lower()

    if any(k in n for k in ["cook", "recipe", "kitchen"]):
        return "Other Non-Fiction"

    if any(k in n for k in ["history", "war", "ww2", "military"]):
        return "History"

    if any(k in n for k in ["physics", "quantum", "chemistry", "biology"]):
        return "Science"

    if any(k in n for k in ["engineer", "mechanic", "circuit", "software", "code"]):
        return "Engineering"

    if any(k in n for k in ["novel", "story", "fiction"]):
        return "Other Fiction"

    if any(k in n for k in ["romance", "love"]):
        return "Romance"

    if any(k in n for k in ["mystery", "detective"]):
        return "Mystery"

    if any(k in n for k in ["horror", "ghost"]):
        return "Horror"

    if any(k in n for k in ["fantasy", "dragon", "magic"]):
        return "Fantasy"

    if any(k in n for k in ["sci-fi", "scifi", "space", "alien"]):
        return "Sci-Fi"

    return None


# --- AI BATCH CLASSIFIER ---
def classify_batch(texts):
    joined = "\n---\n".join(texts)

    prompt = f"""
Classify each item. Output ONLY categories, one per line.

Options:
Sci-Fi, Fantasy, Mystery, Horror, Romance,
Science, Engineering, Medical, History, Philosophy,
Other Fiction, Other Non-Fiction

{joined}
"""

    try:
        res = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )

        lines = res.choices[0].message.content.strip().splitlines()
        return [l.strip() for l in lines]

    except:
        return ["Unknown"] * len(texts)


# --- MOVE ---
def move(path, cat):
    if cat not in [
        "Sci-Fi",
        "Fantasy",
        "Mystery",
        "Horror",
        "Romance",
        "Science",
        "Engineering",
        "Medical",
        "History",
        "Philosophy",
        "Other Fiction",
        "Other Non-Fiction",
    ]:
        cat = "00_Unknown"

    dest = os.path.join(DEST_DIR, cat)
    os.makedirs(dest, exist_ok=True)

    base = os.path.basename(path)
    new = os.path.join(dest, base)

    # If file exists, add number suffix
    count = 1
    name, ext = os.path.splitext(base)

    while os.path.exists(new):
        new = os.path.join(dest, f"{name}_{count}{ext}")
        count += 1

    shutil.move(path, new)

    return new


# --- WORKER ---
def worker(paths):
    global processed

    conn = sqlite3.connect(DB_PATH)

    batch_paths = []
    batch_texts = []

    for path in paths:

        if not os.path.isfile(path):
            continue
        if not valid(path):
            continue
        if cached(conn, path):
            # still move it if it's sitting in a source folder
            row = conn.execute(
                "SELECT category FROM cache WHERE path=?", (path,)
            ).fetchone()
            if row:
                new = move(path, row[0])
                save(conn, new, row[0], None)
            continue

        h = file_hash(path)

        if h:
            d = dup(conn, h)
            if d:
                new = move(path, d[0])
                save(conn, new, d[0], h)
                continue

        q = quick(path)
        if q:
            new = move(path, q)
            save(conn, new, q, h)
            continue

        txt = preview(path)
        if not txt:
            save(conn, path, "Unknown", h)
            continue

        batch_paths.append(path)
        batch_texts.append(txt)

        if len(batch_paths) >= BATCH_SIZE:

            cats = classify_batch(batch_texts)

            for p, c in zip(batch_paths, cats):
                new = move(p, c)
                save(conn, new, c, h)

                with lock:
                    processed += 1
                    if processed % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed
                        print(f"[{processed}] {rate:.2f}/sec")

            batch_paths, batch_texts = [], []

    conn.close()


# --- GATHER ---
def gather():
    for d in SOURCE_DIRS:
        for root, _, files in os.walk(d):
            for f in files:
                yield os.path.join(root, f)


# --- MAIN ---
def main():
    init_db()

    files = list(gather())

    chunk_size = len(files) // MAX_WORKERS

    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        ex.map(worker, chunks)


if __name__ == "__main__":
    main()
