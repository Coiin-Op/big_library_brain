import os
import shutil
import sqlite3
import time
import hashlib
import re
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from openai import OpenAI
from threading import Lock

ai_lock = Lock()

# --- CONFIG ---
SOURCE_DIRS = [r"Z:\organized_books\Unknown"]
DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"

MAX_WORKERS = 3
BATCH_SIZE = 5
MODEL_NAME = "DeepSeek-Coder-V2-Lite-Instruct"
# MODEL_NAME = "mistral-7b-instruct-v0.3.Q4_K_M.gguf"
# MODEL_NAME = "llm/mistralai/mistral-7b-instruct-v0.3"

client = OpenAI(base_url="http://127.0.0.1:8080/v1", api_key="not-needed")

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
    ".py",
    ".cbr",
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


# --- PREVIEW (UPGRADED) ---
def preview(path):
    ext = os.path.splitext(path)[1].lower()

    try:
        if ext == ".txt":
            with open(path, "r", errors="ignore") as f:
                return clean(f.read(500))

        elif ext == ".pdf":
            from pypdf import PdfReader

            reader = PdfReader(path)
            text = ""
            for page in reader.pages[:2]:
                t = page.extract_text()
                if t:
                    text += t
            return clean(text)

        elif ext == ".epub":
            from ebooklib import epub

            book = epub.read_epub(path)
            text = ""
            for item in book.get_items():
                if item.get_type() == 9:
                    text += item.get_content().decode("utf-8", errors="ignore")
                    break
            return clean(text)

    except:
        return ""


# --- QUICK CLASSIFIER ---
def quick(name):
    n = name.lower()

    if n.endswith(".py"):
        return "Python Scripts"

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


def classify_batch(texts):
    joined = "\n---\n".join(texts)

    # HARD clamp input
    joined = joined[:800]

    time.sleep(random.uniform(0.1, 0.4))

    prompt = f"""
Classify each item.

Format:
Category | Confidence

Options:
Sci-Fi, Fantasy, Mystery, Horror, Romance
Science, Engineering, Medical, History, Philosophy
Other Fiction, Other Non-Fiction

{joined}
"""

    # FINAL clamp (includes prompt text)
    prompt = prompt[:1000]

    try:
        with ai_lock:
            # remarked otu to use Anaconda server
            # res = client.chat.completions.create(
            #     model=MODEL_NAME,
            #     messages=[{"role": "user", "content": prompt}],
            #     temperature=0.0,
            #     timeout=30,
            # )
            res = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=200,
                timeout=30,
            )

        results = []
        content = res.choices[0].message.content or ""

        for line in content.split("\n"):
            line = line.strip()
            if not line:
                continue

            try:
                parts = line.split("|")
                if len(parts) == 2:
                    cat, conf = parts
                    cat = cat.strip()
                    conf = float(conf.strip())
                else:
                    raise ValueError("Bad format")

                results.append((cat, conf))

            except Exception as e:
                print(f"[PARSE ERROR] {line} -> {e}")
                results.append(("Other Non-Fiction", 0.0))

        if len(results) != len(texts):
            return [("Other Non-Fiction", 0.0)] * len(texts)

        return results

    except Exception as e:
        print(f"[AI ERROR] {e}")
        return [("Other Non-Fiction", 0.0)] * len(texts)


# --- MOVE ---
def move(path, cat):
    dest = os.path.join(DEST_DIR, cat)
    os.makedirs(dest, exist_ok=True)

    base = os.path.basename(path)
    new = os.path.join(dest, base)

    name, ext = os.path.splitext(base)
    count = 1

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

        ext = os.path.splitext(path)[1].lower()

        # 🔥 Python scripts handled here
        if ext == ".py":
            new = move(path, "Python Scripts")
            save(conn, new, "Python Scripts", None)
            continue

        if cached(conn, path):
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

        # txt = preview(path)

        # if not txt:
        #     new = move(path, "Unsorted")
        #     save(conn, new, "Unsorted", h)
        #     continue
        try:
            txt = preview(path)
        except Exception as e:
            print(f"[PREVIEW ERROR] {path} -> {e}")
            txt = ""

        if not txt or len(txt.strip()) < 50:
            new = move(path, "Needs_Review")
            save(conn, new, "Needs_Review", h)
            continue

        batch_paths.append(path)
        batch_texts.append(txt)

        if len(batch_paths) >= BATCH_SIZE:

            cats = classify_batch(batch_texts)

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

    chunk_size = max(1, len(files) // MAX_WORKERS)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, chunk) for chunk in chunks]

        for f in futures:
            try:
                f.result()
            except Exception as e:
                import traceback

                print("\n=== WORKER CRASH ===")
                traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback

        print("\n=== FATAL ERROR ===")
        traceback.print_exc()
        input("Press Enter to exit...")
