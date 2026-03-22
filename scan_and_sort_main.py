import os
import shutil
import sqlite3
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from openai import OpenAI

print("=== RUNNING THIS FILE ===")

# --- CONFIG ---
SOURCE_DIRS = [r"Z:\organized_books\Unknown"]
DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"

MAX_WORKERS = 3
BATCH_SIZE = 5
MODEL_NAME = "mistralai/mistral-7b-instruct-v0.3"

client = OpenAI(base_url="http://localhost:1234/v1", api_key="local")

lock = Lock()
processed = 0
start_time = time.time()


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


def save(conn, path, category, h):
    conn.execute(
        "INSERT OR REPLACE INTO cache (path, category, hash) VALUES (?, ?, ?)",
        (path, category, h),
    )
    conn.commit()


# --- UTILS ---
def file_hash(path):
    try:
        with open(path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None


def move(path, category):
    dest_dir = os.path.join(DEST_DIR, category)
    os.makedirs(dest_dir, exist_ok=True)
    new_path = os.path.join(dest_dir, os.path.basename(path))
    try:
        shutil.move(path, new_path)
        return new_path
    except:
        return path


def preview(path, max_len=300):
    try:
        with open(path, "r", errors="ignore") as f:
            return f.read(max_len)
    except:
        return ""


# --- AI ---
def classify_batch(texts):
    prompt = "\n\n".join(texts)

    res = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=200,
    )

    content = res.choices[0].message.content or ""

    cats = []
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) >= 1:
            cats.append(parts[0].strip())

    return cats


# --- WORKER ---
def worker(paths):
    global processed

    conn = sqlite3.connect(DB_PATH)
    batch_paths, batch_texts = [], []

    for path in paths:
        txt = preview(path)
        if not txt:
            continue

        batch_paths.append(path)
        batch_texts.append(txt)

        if len(batch_paths) >= BATCH_SIZE == 2:
            cats = classify_batch(batch_texts)

            VALID = {
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
            }

            cats = [c if c in VALID else "Other Non-Fiction" for c in cats]

            for p, c in zip(batch_paths, cats):
                h = file_hash(p)
                new = move(p, c)
                save(conn, new, c, h)

                with lock:
                    processed += 1
                    if processed % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed
                        print(f"[{processed}] {rate:.2f}/sec")

            batch_paths, batch_texts = [], []

    # --- leftover batch ---
    if batch_paths:
        cats = classify_batch(batch_texts)

        VALID = {
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
        }

        cats = [c if c in VALID else "Other Non-Fiction" for c in cats]

        for p, c in zip(batch_paths, cats):
            h = file_hash(p)
            new = move(p, c)
            save(conn, new, c, h)

    conn.close()


# --- GATHER ---
def gather():
    count = 0
    for d in SOURCE_DIRS:
        for root, _, files in os.walk(d):
            for f in files:
                count += 1
                if count % 10000 == 0:
                    print(f"Scanned {count} files...")
                yield os.path.join(root, f)


# --- MAIN ---
def main():
    init_db()

    files = list(gather())
    print(f"FILES FOUND: {len(files)}")

    chunk_size = max(1, len(files) // MAX_WORKERS)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, chunk) for chunk in chunks]

        for f in futures:
            f.result()


if __name__ == "__main__":
    main()
