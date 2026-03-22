import os
import csv
import time
import subprocess
import json
import zipfile
from collections import Counter
from multiprocessing import Pool, cpu_count
from openai import OpenAI
import random

# =========================
# CONFIG (D: ONLY)
# =========================

BASE_DIR = r"D:\library_sorter"
DEST_DIR = r"D:\organized_books"
LOG_CSV = os.path.join(BASE_DIR, "move_plan_full.csv")
CACHE_PATH = os.path.join(BASE_DIR, "classification_cache.jsonl")
SUSPECT_LOG = os.path.join(BASE_DIR, "suspect_files.csv")
STATS_LOG = os.path.join(BASE_DIR, "stats_summary.txt")

RECOLLQ = r"D:\recoll\recollq.exe"

# How many docs to test with (None = all)
TEST_LIMIT = None  # e.g. 500 for testing

# Dry-run: True = do NOT move files, just plan + logs
DRY_RUN = True

# Processes
MAX_PROCESSES = max(4, cpu_count() - 1)

# Local AIs
mixtral_client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")
hadiseh_client = OpenAI(base_url="http://localhost:1235/v1", api_key="lm-studio")


def sample_review(results, count=50):
    """
    Print a random sample of classified files so you can eyeball accuracy.
    """
    if not results:
        print("No results to sample.")
        return

    sample = random.sample(results, min(count, len(results)))

    print("\n=== SAMPLE CLASSIFICATION REVIEW ===")
    for src, cat, conf in sample:
        name = os.path.basename(src)
        print(f"{name:60}  →  {cat:20}  ({conf})")
    print("=== END SAMPLE REVIEW ===\n")


def abort_if_too_many_fallbacks(results, threshold=0.35):
    """
    Abort if fallback classifications exceed the given threshold (default 35%).
    """
    if not results:
        return False

    total = len(results)
    fallback_count = sum(1 for _, _, conf in results if conf == "fallback")
    ratio = fallback_count / total

    print(f"\nFallback ratio: {ratio*100:.2f}% (threshold {threshold*100:.0f}%)")

    if ratio > threshold:
        print("\n*** ABORTING: Too many fallback classifications ***")
        print(f"Fallbacks: {fallback_count:,} out of {total:,}")
        print(
            "This usually means the AI models are not responding or rules are too weak."
        )
        return True

    return False


# =========================
# CATEGORY NORMALIZATION
# =========================

CANONICAL_CATEGORIES = {
    "sci-fi": "Sci-Fi",
    "science fiction": "Sci-Fi",
    "scifi": "Sci-Fi",
    "sf": "Sci-Fi",
    "fantasy": "Fantasy",
    "mystery": "Mystery",
    "crime": "Mystery",
    "thriller": "Mystery",
    "horror": "Horror",
    "romance": "Romance",
    "love story": "Romance",
    "fiction": "Other Fiction",
    "other fiction": "Other Fiction",
    "science": "Science",
    "physics": "Science",
    "chemistry": "Science",
    "biology": "Science",
    "astronomy": "Science",
    "engineering": "Engineering",
    "computer science": "Engineering",
    "programming": "Engineering",
    "software": "Engineering",
    "medical": "Medical",
    "medicine": "Medical",
    "health": "Medical",
    "history": "History",
    "philosophy": "Philosophy",
    "non-fiction": "Other Non-Fiction",
    "non fiction": "Other Non-Fiction",
    "other non-fiction": "Other Non-Fiction",
    "other non fiction": "Other Non-Fiction",
}

FALLBACK_FICTION = "Other Fiction"
FALLBACK_NONFICTION = "Other Non-Fiction"
SCANNED_CATEGORY = "Scanned Books"


def normalize_category(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip().lower()
    s = s.replace("_", " ").replace("-", " ")
    s = " ".join(s.split())

    if s in CANONICAL_CATEGORIES:
        return CANONICAL_CATEGORIES[s]

    if "sci" in s and "fi" in s:
        return "Sci-Fi"
    if "fantasy" in s:
        return "Fantasy"
    if "mystery" in s or "thriller" in s or "crime" in s:
        return "Mystery"
    if "horror" in s:
        return "Horror"
    if "romance" in s or "love" in s:
        return "Romance"
    if "philosophy" in s:
        return "Philosophy"
    if "history" in s:
        return "History"
    if "medical" in s or "medicine" in s or "health" in s:
        return "Medical"
    if "engineering" in s or "programming" in s or "software" in s or "computer" in s:
        return "Engineering"
    if "science" in s:
        return "Science"

    if "non" in s and "fiction" in s:
        return FALLBACK_NONFICTION
    if "fiction" in s:
        return FALLBACK_FICTION

    return None


# =========================
# CACHE
# =========================

classification_cache = {}


def ensure_base_dir():
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(DEST_DIR, exist_ok=True)


def load_cache():
    if not os.path.isfile(CACHE_PATH):
        return
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                key = obj.get("key")
                cat = obj.get("category")
                conf = obj.get("confidence", "unknown")
                if key and cat:
                    classification_cache[key] = (cat, conf)
            except Exception:
                continue


def append_cache(key: str, category: str, confidence: str):
    classification_cache[key] = (category, confidence)
    try:
        with open(CACHE_PATH, "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {"key": key, "category": category, "confidence": confidence},
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        pass


# =========================
# RECOLL DUMP
# =========================


def dump_recoll_text_and_archives(limit=5_000_000):
    """
    Dump ALL text-bearing files + ALL archives from Recoll using JSON output.
    Excludes images, audio, video, executables, DLLs, etc.
    """

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
        "mime:application/zip OR "
        "mime:application/x-rar OR "
        "mime:application/x-7z-compressed OR "
        "mime:application/x-tar OR "
        "mime:application/gzip OR "
        "mime:application/x-bzip2"
    )

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


# =========================
# SHALLOW ARCHIVE INSPECTOR
# =========================

ARCHIVE_MIMES = {
    "application/zip",
    "application/x-rar",
    "application/x-7z-compressed",
    "application/x-tar",
    "application/gzip",
    "application/x-bzip2",
}


def inspect_archive_shallow(path: str, mime: str):
    """
    Shallow inspection: list filenames (if zip), detect book-like extensions.
    No extraction.
    """
    info = {
        "is_archive": mime in ARCHIVE_MIMES,
        "contains_pdf": False,
        "contains_epub": False,
        "contains_mobi": False,
        "contains_txt": False,
        "file_count": 0,
        "top_filenames": [],
    }

    if not info["is_archive"]:
        return info

    # Only handle zip natively; others we just mark as archive
    if mime == "application/zip" and zipfile.is_zipfile(path):
        try:
            with zipfile.ZipFile(path, "r") as zf:
                names = zf.namelist()
                info["file_count"] = len(names)
                info["top_filenames"] = names[:10]
                for n in names:
                    ln = n.lower()
                    if ln.endswith(".pdf"):
                        info["contains_pdf"] = True
                    if ln.endswith(".epub"):
                        info["contains_epub"] = True
                    if (
                        ln.endswith(".mobi")
                        or ln.endswith(".azw")
                        or ln.endswith(".azw3")
                    ):
                        info["contains_mobi"] = True
                    if ln.endswith(".txt"):
                        info["contains_txt"] = True
        except Exception:
            pass

    return info


# =========================
# AI CLASSIFIER
# =========================

CLASSIFY_PROMPT_TEMPLATE = """Classify this book into ONE category:

Fiction:
Sci-Fi, Fantasy, Mystery, Horror, Romance, Other Fiction

Non-Fiction:
Science, Engineering, Medical, History, Philosophy, Other Non-Fiction

Book: {text}

Answer ONLY the category.
"""


def classify_with_mixtral(text):
    prompt = CLASSIFY_PROMPT_TEMPLATE.format(text=text)
    try:
        res = mixtral_client.chat.completions.create(
            model="mixtral-8x7b-instruct-v0.1",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=8,
            timeout=10,
        )
        return res.choices[0].message.content.strip()
    except Exception:
        return None


def classify_with_hadiseh(text):
    prompt = CLASSIFY_PROMPT_TEMPLATE.format(text=text)
    try:
        res = hadiseh_client.chat.completions.create(
            model="Hadiseh-Mhd",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=8,
            timeout=15,
        )
        return res.choices[0].message.content.strip()
    except Exception:
        return None


# =========================
# RULES + HEURISTICS
# =========================


def looks_like_fiction(name, title):
    base = title or name
    if " - " in base:
        return True
    words = base.replace("_", " ").split()
    if 2 <= len(words) <= 8:
        tech = [
            "guide",
            "manual",
            "introduction",
            "handbook",
            "programming",
            "reference",
        ]
        if not any(w.lower() in tech for w in words):
            return True
    return False


def fast_rule_category(name, title, mime, archive_info):
    n = name.lower()
    t = (title or "").lower()
    m = (mime or "").lower()

    rules = [
        (["for dummies"], "Other Non-Fiction"),
        (["ai", "machine learning"], "Science"),
        (["deep learning"], "Science"),
        (["finance", "trading", "stocks", "crypto"], "Other Non-Fiction"),
        (["history", "war", "battle"], "History"),
        (["biology", "medical", "health", "anatomy"], "Medical"),
        (["programming", "computer", "software", "coding", "developer"], "Engineering"),
        (["forensic", "crime"], "Science"),
    ]

    for keys, cat in rules:
        if any(k in n or k in t for k in keys):
            return cat, "rule"

    # Archive-based hints
    if archive_info["is_archive"]:
        if archive_info["contains_epub"] or archive_info["contains_mobi"]:
            if looks_like_fiction(name, title):
                return FALLBACK_FICTION, "rule"
        if (
            archive_info["contains_pdf"]
            and not archive_info["contains_epub"]
            and not archive_info["contains_mobi"]
        ):
            if any(k in n for k in ["manual", "guide", "reference"]):
                return FALLBACK_NONFICTION, "rule"

    # MIME-based hints
    if "epub" in m or "mobi" in m:
        if looks_like_fiction(name, title):
            return FALLBACK_FICTION, "rule"

    return None, None


# =========================
# SUSPECT FILE DETECTION
# =========================


def log_suspect(path, reason, mime, dbytes):
    header_needed = not os.path.isfile(SUSPECT_LOG)
    with open(SUSPECT_LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if header_needed:
            w.writerow(["path", "reason", "mime", "bytes"])
        w.writerow([path, reason, mime, dbytes])


# =========================
# PROCESS ONE DOC
# =========================


def process_doc(doc):
    path = doc["path"]
    title = doc.get("title") or ""
    dbytes = doc.get("dbytes", 0)
    mime = doc.get("mime") or ""
    name = os.path.basename(path)
    n = name.lower()

    # Skip obvious junk names
    if "file____" in n or "____" in n:
        log_suspect(path, "junk_name", mime, dbytes)
        return None

    if not os.path.isfile(path):
        log_suspect(path, "missing_file", mime, dbytes)
        return None

    if dbytes == 0:
        log_suspect(path, "zero_bytes", mime, dbytes)
        return (path, SCANNED_CATEGORY, "rule")

    archive_info = inspect_archive_shallow(path, mime)

    key_text = title.strip() if title.strip() else name
    cache_key = f"{key_text}||{mime}||{archive_info['is_archive']}"

    if cache_key in classification_cache:
        cat, conf = classification_cache[cache_key]
        return (path, cat, conf)

    cat, conf = fast_rule_category(name, title, mime, archive_info)
    if cat:
        append_cache(cache_key, cat, conf)
        return (path, cat, conf)

    text = key_text

    cat1 = classify_with_mixtral(text)
    norm1 = normalize_category(cat1) if cat1 else None
    if norm1:
        append_cache(cache_key, norm1, "mixtral")
        return (path, norm1, "mixtral")

    cat2 = classify_with_hadiseh(text)
    norm2 = normalize_category(cat2) if cat2 else None
    if norm2:
        append_cache(cache_key, norm2, "hadiseh")
        return (path, norm2, "hadiseh")

    if looks_like_fiction(name, title):
        cat = FALLBACK_FICTION
    else:
        cat = FALLBACK_NONFICTION

    append_cache(cache_key, cat, "fallback")
    return (path, cat, "fallback")


# =========================
# STATS
# =========================


def print_and_log_initial_stats(docs):
    lines = []
    lines.append("=== INITIAL LIBRARY STATS ===")

    mime_counter = Counter(doc.get("mime", "unknown") for doc in docs)
    lines.append("\nTop MIME types:")
    for mime, count in mime_counter.most_common(30):
        lines.append(f"  {mime:45} {count:,}")

    sizes = [doc.get("dbytes", 0) for doc in docs if doc.get("dbytes", 0) > 0]
    if sizes:
        sizes_sorted = sorted(sizes)
        avg = sum(sizes) / len(sizes)
        median = sizes_sorted[len(sizes_sorted) // 2]
        lines.append("\nFile size stats:")
        lines.append(f"  Count:   {len(sizes):,}")
        lines.append(f"  Average: {avg/1024/1024:.2f} MB")
        lines.append(f"  Median:  {median/1024/1024:.2f} MB")
        lines.append(f"  Min:     {min(sizes)/1024/1024:.2f} MB")
        lines.append(f"  Max:     {max(sizes)/1024/1024:.2f} MB")

    archive_count = sum(1 for d in docs if d.get("mime") in ARCHIVE_MIMES)
    text_count = len(docs) - archive_count
    lines.append("\nArchive vs Text:")
    lines.append(f"  Archives: {archive_count:,}")
    lines.append(f"  Text:     {text_count:,}")

    lines.append("\n=== END INITIAL STATS ===\n")

    print("\n".join(lines))

    with open(STATS_LOG, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def print_and_log_category_stats(results):
    lines = []
    lines.append("=== CATEGORY STATS ===")

    cat_counter = Counter()
    conf_counter = Counter()

    for _, cat, conf in results:
        cat_counter[cat] += 1
        conf_counter[conf] += 1

    lines.append("\nBy category:")
    for cat, count in cat_counter.most_common():
        lines.append(f"  {cat:20} {count:,}")

    lines.append("\nBy confidence source:")
    for conf, count in conf_counter.most_common():
        lines.append(f"  {conf:10} {count:,}")

    lines.append("\n=== END CATEGORY STATS ===\n")

    print("\n".join(lines))

    with open(STATS_LOG, "a", encoding="utf-8") as f:
        f.write("\n\n")
        f.write("\n".join(lines))


# =========================
# BUILD MOVE PLAN + MOVE
# =========================


def build_move_plan(docs, workers=MAX_PROCESSES):
    start = time.time()
    total = len(docs)
    print(f"Processing {total:,} docs with {workers} processes...")

    results = []

    with Pool(processes=workers) as pool:
        for i, res in enumerate(pool.imap_unordered(process_doc, docs), 1):
            if res is not None:
                results.append(res)

            if i % 50 == 0 or i == total:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                print(f"[{i:,}/{total:,}] {rate:.1f} docs/sec")

    return results


def write_move_plan_csv(results, csv_path=LOG_CSV):
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "category", "dest", "confidence"])
        for src, cat, conf in results:
            dest_dir = os.path.join(DEST_DIR, cat)
            dest_path = os.path.join(dest_dir, os.path.basename(src))
            w.writerow([src, cat, dest_path, conf])
    print(f"Wrote move plan for {len(results):,} files to {csv_path}")


def move_files_from_plan(csv_path=LOG_CSV, dry_run=True):
    if not os.path.isfile(csv_path):
        print(f"No move plan found at {csv_path}")
        return

    moved = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = row["source"]
            dest = row["dest"]
            if not os.path.isfile(src):
                continue
            dest_dir = os.path.dirname(dest)
            if not dry_run:
                os.makedirs(dest_dir, exist_ok=True)
                try:
                    os.replace(src, dest)
                    moved += 1
                except Exception as e:
                    print(f"Failed to move {src} -> {dest}: {e}")
            else:
                moved += 1

    if dry_run:
        print(f"[DRY RUN] Would move {moved:,} files based on {csv_path}")
    else:
        print(f"Moved {moved:,} files based on {csv_path}")

    # ---------------------------------------------
    # def review_by_category(results, samples_per_cat=10):
    # --------------------------------------------------
    """
    Print grouped samples by category for human review.
    """
    if not results:
        print("No results to review.")
        return

    # Group results by category
    grouped = {}
    for src, cat, conf in results:
        grouped.setdefault(cat, []).append((src, conf))

    print("\n=== REVIEW BY CATEGORY ===")
    for cat, items in grouped.items():
        print(f"\n--- {cat} (showing up to {samples_per_cat}) ---")
        sample = (
            items[:samples_per_cat]
            if len(items) <= samples_per_cat
            else random.sample(items, samples_per_cat)
        )
        for src, conf in sample:
            name = os.path.basename(src)
            print(f"{name:60}  →  {cat:20}  ({conf})")
    print("\n=== END REVIEW BY CATEGORY ===\n")

    # Ask user if the classification looks correct
    ans = input("Does this look correct? (y/n): ").strip().lower()
    if ans != "y":
        print("Stopping before writing move plan or moving files.")
        return


def review_by_category(results, samples_per_cat=10):
    """
    Print grouped samples by category for human review.
    """
    if not results:
        print("No results to review.")
        return

    # Group results by category
    grouped = {}
    for src, cat, conf in results:
        grouped.setdefault(cat, []).append((src, conf))

    print("\n=== REVIEW BY CATEGORY ===")
    for cat, items in grouped.items():
        print(f"\n--- {cat} (showing up to {samples_per_cat}) ---")
        sample = (
            items[:samples_per_cat]
            if len(items) <= samples_per_cat
            else random.sample(items, samples_per_cat)
        )
        for src, conf in sample:
            name = os.path.basename(src)
            print(f"{name:60}  →  {cat:20}  ({conf})")
    print("\n=== END REVIEW BY CATEGORY ===\n")


# =========================
# MAIN
# =========================


def main():
    ensure_base_dir()

    print("Loading classification cache...")
    load_cache()
    print(f"Cache has {len(classification_cache):,} entries.")

    print("Dumping Recoll text + archives via JSON...")
    docs = dump_recoll_text_and_archives()
    print(f"Recoll returned {len(docs):,} files total.")

    if TEST_LIMIT is not None:
        docs = docs[:TEST_LIMIT]
        print(f"Testing with first {len(docs):,} files...")

    print_and_log_initial_stats(docs)

    # results = build_move_plan(docs)
    # print_and_log_category_stats(results)
    # write_move_plan_csv(results)
    # move_files_from_plan(LOG_CSV, dry_run=DRY_RUN)

    results = build_move_plan(docs)
    print_and_log_category_stats(results)

    # Abort if fallback ratio is too high
    if abort_if_too_many_fallbacks(results, threshold=0.35):
        return

    # Human review
    sample_review(results, count=50)
    review_by_category(results, samples_per_cat=10)

    ans = input("Does this look correct? (y/n): ").strip().lower()
    if ans != "y":
        print("Stopping before writing move plan or moving files.")
        return

    write_move_plan_csv(results)
    move_files_from_plan(LOG_CSV, dry_run=DRY_RUN)


if __name__ == "__main__":
    main()
