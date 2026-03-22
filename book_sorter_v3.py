import os
import shutil
import sqlite3
import time
import subprocess
import pathlib
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# --- OPTIONAL PDF IMPROVEMENT ---
try:
    import fitz  # pymupdf
    USE_PYMUPDF = True
except:
    from pypdf import PdfReader
    USE_PYMUPDF = False

# --- CONFIG ---
SOURCE_QUERY = "ext:pdf"
DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"
MAX_WORKERS = 6
RECOLL_PATH = r"D:\recoll\recollq.exe"
MODEL_NAME = "mixtral"

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

# --- CATEGORY MAP ---
FICTION = ["Sci-Fi", "Fantasy", "Mystery", "Horror", "Romance", "Other Fiction"]
NONFICTION = ["Science", "Engineering", "Medical", "History", "Philosophy", "Other Non-Fiction"]

ALL_CATEGORIES = FICTION + NONFICTION

NORMALIZE = {c.lower(): c for c in ALL_CATEGORIES}


# --- DB ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            path TEXT PRIMARY KEY,
            category TEXT,
            hash TEXT
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hash ON cache(hash)
    """)

    conn.commit()
    conn.close()


def get_cached(conn, path):
    c = conn.cursor()
    c.execute("SELECT category FROM cache WHERE path=?", (path,))
    row = c.fetchone()
    return row[0] if row else None


def save_cache(conn, path, category, file_hash):
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, ?)", (path, category, file_hash))
    conn.commit()


def find_duplicate(conn, file_hash):
    c = conn.cursor()
    c.execute("SELECT path, category FROM cache WHERE hash=?", (file_hash,))
    return c.fetchone()


# --- HASH ---
def file_hash(path):
    try:
        hasher = hashlib.md5()
        with open(path, "rb") as f:
            hasher.update(f.read(4096))  # partial hash for speed
        return hasher.hexdigest()
    except:
        return None


# --- TEXT EXTRACTION ---
def extract_preview(path, max_chars=1200):
    try:
        if path.lower().endswith(".pdf"):
            if USE_PYMUPDF:
                doc = fitz.open(path)
                text = ""
                for page in doc[:2]:
                    text += page.get_text()
                return text[:max_chars]
            else:
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
def get_recoll_batch(query, offset, size=1000):
    cmd = [RECOLL_PATH, "-t", "-n", str(size), "-S", str(offset), query]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")

    files = []
    for line in result.stdout.splitlines():
        if "file:///" in line:
            try:
                path = line.split("file:///")[1].split("]")[0]
                if pathlib.Path(path).exists():
                    files.append(path)
            except:
                pass

    return list(dict.fromkeys(files))


def recoll_all_files(query):
    offset = 0
    while True:
        batch = get_recoll_batch(query, offset)
        if not batch:
            break
        print(f"[RECOLL] Batch {offset} → {len(batch)} files")
        yield batch
        offset += 1000


# --- AI CLASSIFICATION ---
def classify_fiction_type(name, text):
    prompt = f"""
Is this Fiction or Non-Fiction?

Book: {name}
Preview: {text}

Answer ONLY: Fiction or Non-Fiction
"""
    res = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return res.choices[0].message.content.strip()


def classify_subcategory(name, text, category_type):
    options = FICTION if category_type == "Fiction" else NONFICTION

    prompt = f"""
Choose EXACTLY one:

{options}

Book: {name}
Preview: {text}

Answer ONLY category.
"""
    res = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )

    raw = res.choices[0].message.content.strip().lower()
    return NORMALIZE.get(raw, "Unknown")


# --- MOVE ---
def move_file(path, category):
    try:
        dest = os.path.join(DEST_DIR, category)
        os.makedirs(dest, exist_ok=True)

        new_path = os.path.join(dest, os.path.basename(path))

        if not os.path.exists(new_path):
            shutil.move(path, new_path)

        return new_path
    except Exception as e:
        print(f"[MOVE ERROR] {e}")
        return path


# --- WORKER ---
def process_file(path):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    try:
        if not os.path.isfile(path):
            return

        cached = get_cached(conn, path)
        if cached:
            print(f"[CACHE] {os.path.basename(path)} → {cached}")
            return

        h = file_hash(path)
        if h:
            dup = find_duplicate(conn, h)
            if dup:
                print(f"[DUPLICATE] {path} → using {dup[1]}")
                save_cache(conn, path, dup[1], h)
                move_file(path, dup[1])
                return

        preview = extract_preview(path)
        if not preview.strip():
            save_cache(conn, path, "Unknown", h)
            return

        # --- 2 PASS CLASSIFICATION ---
        main_type = classify_fiction_type(path, preview)
        category = classify_subcategory(path, preview, main_type)

        save_cache(conn, path, category, h)

        print(f"{os.path.basename(path)} → {category}")

        move_file(path, category)

        time.sleep(0.2)

    finally:
        conn.close()


# --- UNKNOWN REPROCESS ---
def reprocess_unknowns():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT path FROM cache WHERE category='Unknown'")
    paths = [row[0] for row in c.fetchall()]

    conn.close()

    print(f"[REPROCESS] {len(paths)} unknowns")

    for p in paths:
        process_file(p)


# --- MAIN ---
def main():
    init_db()

    for batch in recoll_all_files(SOURCE_QUERY):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_file, f) for f in batch]

            for _ in as_completed(futures):
                pass

    # second pass cleanup
    reprocess_unknowns()


if __name__ == "__main__":
    main()