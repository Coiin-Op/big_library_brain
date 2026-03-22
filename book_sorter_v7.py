import os
import shutil
import sqlite3
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# --- OPTIONAL PDF SUPPORT ---
try:
    import fitz

    USE_PYMUPDF = True
except:
    from pypdf import PdfReader

    USE_PYMUPDF = False

# --- CONFIG ---
SOURCE_DIRS = [r"Z:\The Library", r"Z:\The Library 2"]

DEST_DIR = r"Z:\organized_books"
DB_PATH = "classification_cache.db"
MAX_WORKERS = 5

MODEL_NAME = "mistral"  # MUST match LM Studio exactly

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

# --- FILE FILTER ---
ALLOWED_EXTENSIONS = {
    ".pdf",
    ".txt",
    ".md",
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

BLOCKED_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".jpg",
    ".jpeg",
    ".png",
    ".bmp",
    ".gif",
    ".webp",
    ".exe",
    ".dll",
    ".bin",
    ".iso",
}


def is_valid_book_file(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in BLOCKED_EXTENSIONS:
        return False
    if ext not in ALLOWED_EXTENSIONS:
        return False
    try:
        if os.path.getsize(path) < 1024:
            return False
    except:
        return False
    return True


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

    conn.execute("CREATE INDEX IF NOT EXISTS idx_hash ON cache(hash)")
    conn.commit()
    conn.close()


def get_cached(conn, path):
    c = conn.cursor()
    c.execute("SELECT category FROM cache WHERE path=?", (path,))
    row = c.fetchone()
    return row[0] if row else None


def save_cache(conn, path, category, file_hash):
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO cache VALUES (?, ?, ?)", (path, category, file_hash)
    )
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
            hasher.update(f.read(4096))
        return hasher.hexdigest()
    except:
        return None


# --- TEXT EXTRACTION ---
def extract_preview(path, max_chars=600):
    try:
        ext = path.lower()

        if ext.endswith(".pdf"):
            if USE_PYMUPDF:
                doc = fitz.open(path)
                return "".join(p.get_text() for p in doc[:2])[:max_chars]
            else:
                reader = PdfReader(path)
                return "".join(p.extract_text() or "" for p in reader.pages[:2])[
                    :max_chars
                ]

        elif ext.endswith((".txt", ".md", ".xml")):
            with open(path, "r", errors="ignore") as f:
                return f.read(max_chars)

        elif ext.endswith((".html", ".htm")):
            import re

            with open(path, "r", errors="ignore") as f:
                raw = f.read(max_chars * 3)
            text = re.sub("<[^<]+?>", " ", raw)
            return re.sub(r"\s+", " ", text)[:max_chars]

        elif ext.endswith(".epub"):
            import zipfile

            text = ""
            with zipfile.ZipFile(path, "r") as z:
                for name in z.namelist():
                    if name.endswith((".html", ".xhtml")):
                        text += z.read(name).decode(errors="ignore")
                        if len(text) > max_chars:
                            break
            return text[:max_chars]

        else:
            with open(path, "r", errors="ignore") as f:
                return f.read(max_chars)

    except:
        return ""


# --- CATEGORIES ---
CATEGORIES = [
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
]

NORMALIZE = {c.lower(): c for c in CATEGORIES}


# --- CLASSIFIER ---
def classify_book(name, text):
    try:
        prompt = f"""
Choose EXACTLY one category from this list:

{CATEGORIES}

Book: {name}
Preview: {text}

Answer ONLY the category.
"""
        res = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )

        raw = res.choices[0].message.content.strip().lower()
        return NORMALIZE.get(raw, "Unknown")

    except:
        return "Unknown"


# --- MOVE ---
def move_file(path, category):
    try:
        dest = os.path.join(DEST_DIR, category)
        os.makedirs(dest, exist_ok=True)

        new_path = os.path.join(dest, os.path.basename(path))

        if not os.path.exists(new_path):
            shutil.move(path, new_path)

        return new_path
    except:
        return path


# --- WALK ---
def gather_files():
    for root_dir in SOURCE_DIRS:
        for root, _, files in os.walk(root_dir):
            for f in files:
                yield os.path.join(root, f)


# --- WORKER ---
def process_file(path):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    try:
        if not os.path.isfile(path):
            return

        if not is_valid_book_file(path):
            return

        print(f"[SCAN] {os.path.basename(path)}")

        if get_cached(conn, path):
            return

        h = file_hash(path)

        if h:
            dup = find_duplicate(conn, h)
            if dup:
                new_path = move_file(path, dup[1])
                save_cache(conn, new_path, dup[1], h)
                return

        preview = extract_preview(path)

        if not preview.strip():
            save_cache(conn, path, "Unknown", h)
            return

        category = classify_book(path, preview)

        new_path = move_file(path, category)
        save_cache(conn, new_path, category, h)

        print(f"{os.path.basename(path)} → {category}")

        time.sleep(0.05)

    finally:
        conn.close()


# --- MAIN ---
def main():
    init_db()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, f) for f in gather_files()]
        for _ in as_completed(futures):
            pass


if __name__ == "__main__":
    main()
