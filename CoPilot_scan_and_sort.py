import os
import csv
import time
import subprocess
import json
from multiprocessing import Pool, cpu_count
from openai import OpenAI

# --- CONFIG ---
DEST_DIR = r"Z:\organized_books"
LOG_CSV = "move_plan_test200.csv"
RECOLLQ = r"D:\recoll\recollq.exe"
MAX_PROCESSES = max(4, cpu_count() - 2)
TEST_LIMIT = 200  # limit for first run

# AI clients
mixtral_client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")
hadiseh_client = OpenAI(base_url="http://localhost:1235/v1", api_key="lm-studio")


# --- RECOLL DUMP (JSON, STABLE) ---
def dump_recoll_text_and_archives(limit=5_000_000):
    """
    Dump ALL text-bearing files + ALL archives from Recoll using JSON output.
    Excludes images, audio, video, executables, DLLs, etc.
    """

    # Include all text-like formats + archives
    include = (
        "mime:text OR "
        "mime:application/pdf OR "
        "mime:application/epub+zip OR "
        "mime:application/msword OR "
        "mime:application/vnd.openxmlformats-officedocument* OR "
        "mime:application/rtf OR "
        "mime:application/json OR "
        "mime:application/xml OR "
        "mime:application/x-chm OR "
        "mime:application/x-mobipocket-ebook OR "
        "mime:message/rfc822 OR "
        "mime:inode/x-empty OR "
        # ARCHIVES (you want these)
        "mime:application/zip OR "
        "mime:application/x-rar OR "
        "mime:application/x-7z-compressed OR "
        "mime:application/x-tar OR "
        "mime:application/gzip OR "
        "mime:application/x-bzip2"
    )

    # Exclude non-text formats
    exclude = (
        "mime:image OR "
        "mime:audio OR "
        "mime:video OR "
        "mime:application/x-executable OR "
        "mime:application/x-dosexec OR "
        "mime:application/x-msdownload OR "
        "mime:application/x-sharedlib OR "
        "mime:application/x-object OR "
        "mime:application/x-binary"
    )

    query = f"({include}) AND NOT ({exclude})"

    cmd = [
        RECOLLQ,
        "-F",
        "json",
        "-n",
        str(limit),
        query,
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )

    if result.returncode != 0:
        print("recollq failed:", result.stderr.strip())
        return []

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("Failed to parse recollq JSON output")
        return []

    docs = []

    for doc in data:
        url = doc.get("url") or doc.get("rcl_url") or ""
        title = doc.get("title") or doc.get("rcl_title") or ""
        mime = doc.get("mime") or doc.get("rcl_mime") or ""
        size = doc.get("size") or doc.get("rcl_size") or 0
        author = doc.get("author") or doc.get("rcl_author")
        score = doc.get("rcl_score")

        # Convert file:/// path
        if url.startswith("file:///"):
            path = url.replace("file:///", "")
        else:
            path = url

        if not path or not os.path.isfile(path):
            continue

        try:
            dbytes = int(size)
        except Exception:
            dbytes = 0

        docs.append(
            {
                "path": path,
                "title": title,
                "dbytes": dbytes,
                "author": author,
                "score": score,
                "mime": mime,
            }
        )

    return docs


# --- AI HELPERS ---
def classify_with_mixtral(text):
    prompt = f"""
Classify this book into ONE category:

Fiction:
Sci-Fi, Fantasy, Mystery, Horror, Romance, Other Fiction

Non-Fiction:
Science, Engineering, Medical, History, Philosophy, Other Non-Fiction

Book: {text}

Answer ONLY the category.
"""
    try:
        res = mixtral_client.chat.completions.create(
            model="mixtral-8x7b-instruct-v0.1",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=8,
            timeout=5,
        )
        return res.choices[0].message.content.strip()
    except Exception:
        return None


def classify_with_hadiseh(text):
    prompt = f"""
Classify this book into ONE category:

Fiction:
Sci-Fi, Fantasy, Mystery, Horror, Romance, Other Fiction

Non-Fiction:
Science, Engineering, Medical, History, Philosophy, Other Non-Fiction

Book: {text}

Answer ONLY the category.
"""
    try:
        res = hadiseh_client.chat.completions.create(
            model="Hadiseh-Mhd",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=8,
            timeout=8,
        )
        return res.choices[0].message.content.strip()
    except Exception:
        return None


# --- RULES ---
def fast_rule_category(name, title):
    n = name.lower()
    t = (title or "").lower()

    rules = [
        (["for dummies"], "Other Non-Fiction"),
        (["ai", "machine learning"], "Science"),
        (["finance", "trading", "stocks", "crypto"], "Other Non-Fiction"),
        (["history", "war", "battle"], "History"),
        (["biology", "medical", "health"], "Medical"),
        (["programming", "computer", "software", "coding"], "Engineering"),
        (["forensic", "crime"], "Science"),
    ]

    for keys, cat in rules:
        if any(k in n or k in t for k in keys):
            return cat

    return None


def looks_like_fiction(name, title):
    base = title or name
    if " - " in base:
        return True
    words = base.replace("_", " ").split()
    if 2 <= len(words) <= 8:
        tech = ["guide", "manual", "introduction", "handbook", "programming"]
        if not any(w.lower() in tech for w in words):
            return True
    return False


# --- PROCESS ONE DOC ---
def process_doc(doc):
    path = doc["path"]
    title = doc.get("title") or ""
    dbytes = doc.get("dbytes", 0)
    name = os.path.basename(path)
    n = name.lower()

    if "file____" in n or "____" in n:
        return None

    if dbytes == 0:
        return (path, "Scanned Books")

    cat = fast_rule_category(name, title)
    if cat:
        return (path, cat)

    text = title if title.strip() else name

    cat1 = classify_with_mixtral(text)
    if cat1 and "other" not in cat1.lower():
        return (path, cat1)

    cat2 = classify_with_hadiseh(text)
    if cat2:
        return (path, cat2)

    if looks_like_fiction(name, title):
        return (path, "Other Fiction")
    return (path, "Other Non-Fiction")


# --- BATCH PIPELINE ---
def build_move_plan(docs, workers=MAX_PROCESSES):
    start = time.time()
    total = len(docs)
    print(f"Processing {total} docs with {workers} processes...")

    results = []
    with Pool(processes=workers) as pool:
        for i, res in enumerate(pool.imap_unordered(process_doc, docs), 1):
            if res is not None:
                results.append(res)

            if i % 50 == 0 or i == total:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                print(f"[{i}/{total}] {rate:.1f} docs/sec")

    return results


def write_move_plan_csv(pairs, csv_path=LOG_CSV):
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source", "category", "dest"])
        for src, cat in pairs:
            dest_dir = os.path.join(DEST_DIR, cat)
            dest_path = os.path.join(dest_dir, os.path.basename(src))
            writer.writerow([src, cat, dest_path])
    print(f"Wrote move plan for {len(pairs)} files to {csv_path}")


# --- MAIN ---
def main():
    print("Dumping Recoll PDFs via JSON...")
    docs = dump_recoll_pdfs()
    print(f"Recoll returned {len(docs)} files total.")
    docs = docs[:TEST_LIMIT]
    print(f"Testing with first {len(docs)} files...")

    pairs = build_move_plan(docs)
    write_move_plan_csv(pairs)


if __name__ == "__main__":
    main()
