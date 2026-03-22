# file: scan_and_sort.py

import os
import shutil
import time
import subprocess
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# --- CONFIG ---
DEST_DIR = r"Z:\organized_books"
LOG_FILE = "move_log.txt"
MAX_WORKERS = 1

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")


# --- MOVE FILE ---
def move_file(src, category):
    dest_path = os.path.join(DEST_DIR, category)
    os.makedirs(dest_path, exist_ok=True)

    filename = os.path.basename(src)
    dest = os.path.join(dest_path, filename)

    try:
        shutil.move(src, dest)

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{src} -> {dest}\n")

    except Exception as e:
        print(f"Move failed: {e}")


# --- CHECK IF PDF HAS TEXT ---
def has_text(path):
    try:
        from pypdf import PdfReader

        reader = PdfReader(path)

        text = ""
        for page in reader.pages[:3]:
            t = page.extract_text()
            if t:
                text += t

        return len(text.strip()) > 50
    except:
        return False


def process_library():
    start_time = time.time()

    root = pathlib.Path(r"Z:\The Library")

    files = list(root.rglob("*.pdf"))  # 🔥 recursive scan

    if not files:
        print("No files found")
        return

    print(f"Found {len(files)} files")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, str(f)) for f in files]

        total = len(files)

        for i, _ in enumerate(as_completed(futures), 1):
            elapsed = time.time() - start_time
            avg = elapsed / i
            remaining = avg * (total - i)

            print(
                f"[{i}/{total}] {int((i/total)*100)}% | "
                f"{elapsed:.1f}s | ETA {remaining:.1f}s"
            )


# --- AI CLASSIFIER (RARE USE) ---
def classify_book(name, retries=2):
    prompt = f"""
Classify into ONE category:

Fiction:
Sci-Fi, Fantasy, Mystery, Horror, Romance, Other Fiction

Non-Fiction:
Science, Engineering, Medical, History, Philosophy, Other Non-Fiction

Book: {name}

Answer ONLY category.
"""

    for _ in range(retries):
        try:
            res = client.chat.completions.create(
                model="mixtral-8x7b-instruct-v0.1",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=5,
                timeout=5,
            )
            return res.choices[0].message.content.strip()
        except:
            time.sleep(1)

    return "Other Non-Fiction"


# --- WORKER ---
def process_file(path):
    file_start = time.time()

    print(f"[START] {path}")

    try:
        if not os.path.isfile(path):
            return

        name = os.path.basename(path)
        n = name.lower()

        # --- SKIP JUNK ---
        if "file____" in n or "____" in n:
            print("→ Skipped (junk)")
            return

        category = None

        # --- FAST RULES ---
        if "for dummies" in n:
            category = "Other Non-Fiction"

        elif any(x in n for x in ["ai", "machine learning"]):
            category = "Science"

        elif any(x in n for x in ["finance", "trading", "stocks", "crypto"]):
            category = "Other Non-Fiction"

        elif any(x in n for x in ["history", "war", "battle"]):
            category = "History"

        elif any(x in n for x in ["biology", "medical", "health"]):
            category = "Medical"

        elif any(x in n for x in ["programming", "computer", "software"]):
            category = "Engineering"

        elif any(x in n for x in ["forensic", "crime"]):
            category = "Science"

        # --- SCANNED CHECK ---
        if category is None and not has_text(path):
            category = "Scanned Books"

        # --- AI (RARE) ---
        if category is None:
            if " - " in name:  # looks like fiction
                category = classify_book(name)
            else:
                category = "Other Non-Fiction"

        # --- FINAL SAFETY ---
        if not category:
            category = "Other Non-Fiction"

        duration = time.time() - file_start

        print(f"[DONE] {name} → {category} ({duration:.2f}s)")

        move_file(path, category)

    except Exception as e:
        print(f"ERROR: {e}")


# --- MAIN ---
def process_recoll():
    start_time = time.time()

    files = get_recoll_files("ext:pdf")[:200]

    if not files:
        print("No files found")
        return

    print(f"Found {len(files)} files")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, f) for f in files]

        total = len(files)

        for i, _ in enumerate(as_completed(futures), 1):
            elapsed = time.time() - start_time
            avg = elapsed / i
            remaining = avg * (total - i)

            print(
                f"[{i}/{total}] {int((i/total)*100)}% | "
                f"{elapsed:.1f}s | ETA {remaining:.1f}s"
            )


if __name__ == "__main__":
    process_library()
