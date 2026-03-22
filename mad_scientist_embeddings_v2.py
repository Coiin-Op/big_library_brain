import os
import sqlite3
import json
import time
import numpy as np
from openai import OpenAI
from sklearn.cluster import MiniBatchKMeans
import re


def strip_html(text):
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


DB_PATH = "classification_cache.db"
EMBED_DB = "embeddings.db"
MODEL = "nomic-embed-text"
BATCH_SIZE = 32
NUM_CLUSTERS = 50

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")


# --- DB ---
def init_db():
    conn = sqlite3.connect(EMBED_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS embeddings (
            path TEXT PRIMARY KEY,
            vector TEXT,
            cluster INTEGER
        )
    """
    )
    conn.commit()
    conn.close()


def move_by_cluster():
    import shutil

    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    c.execute("SELECT path, cluster FROM embeddings WHERE cluster IS NOT NULL")
    rows = c.fetchall()

    for path, cluster in rows:
        if not os.path.exists(path):
            continue

        folder = os.path.join("Z:\\organized_books", f"cluster_{cluster}")
        os.makedirs(folder, exist_ok=True)

        base = os.path.basename(path)
        dest = os.path.join(folder, base)

        count = 1
        name, ext = os.path.splitext(base)

        while os.path.exists(dest):
            dest = os.path.join(folder, f"{name}_{count}{ext}")
            count += 1

        try:
            shutil.move(path, dest)
        except:
            pass

    conn.close()


# --- LOAD BOOKS ---
def load_books():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT path FROM cache WHERE category='Unknown' OR category='00_Unknown'"
    )
    paths = [row[0] for row in c.fetchall()]
    conn.close()
    return paths


# --- PREVIEW ---
def get_preview(path, max_chars=800):
    ext = os.path.splitext(path)[1].lower()

    try:
        # --- TEXT FILES (GOOD) ---
        if ext in [".txt", ".html", ".htm", ".rtf"]:
            with open(path, "r", errors="ignore") as f:
                return f.read(max_chars)

                # --- EPUB (ZIP) ---
        elif ext == ".epub":
            import zipfile

            with zipfile.ZipFile(path, "r") as z:
                for name in z.namelist():
                    if name.endswith((".html", ".xhtml", ".htm")):
                        with z.open(name) as f:
                            content = f.read(max_chars).decode("utf-8", errors="ignore")
                            return strip_html(content)

        # --- FALLBACK: USE FILENAME ---
        return os.path.basename(path)

    except:
        return os.path.basename(path)


# --- EMBEDDING ---
def embed_batch(texts):
    res = client.embeddings.create(model=MODEL, input=texts)
    return [e.embedding for e in res.data]


# --- SAVE EMBEDDINGS ---
def save_embeddings(batch_paths, vectors):
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    for p, v in zip(batch_paths, vectors):
        c.execute(
            "INSERT OR REPLACE INTO embeddings VALUES (?, ?, NULL)", (p, json.dumps(v))
        )

    conn.commit()
    conn.close()


# --- GENERATE ---
def generate_embeddings(paths):
    batch = []
    batch_paths = []

    for path in paths:
        preview = get_preview(path)

        if not preview.strip():
            continue

        batch.append(preview)
        batch_paths.append(path)

        if len(batch) >= BATCH_SIZE:
            vectors = embed_batch(batch)
            save_embeddings(batch_paths, vectors)

            print(f"[EMBED] {len(batch)}")

            batch, batch_paths = [], []
            time.sleep(0.2)

    if batch:
        vectors = embed_batch(batch)
        save_embeddings(batch_paths, vectors)


# --- LOAD VECTORS ---
def load_vectors():
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    c.execute("SELECT path, vector FROM embeddings")
    rows = c.fetchall()
    conn.close()

    paths = []
    vectors = []

    for p, v in rows:
        paths.append(p)
        vectors.append(json.loads(v))

    return paths, np.array(vectors)


# --- CLUSTER ---
def cluster_vectors(vectors):
    print("[CLUSTERING]")
    kmeans = MiniBatchKMeans(n_clusters=NUM_CLUSTERS, batch_size=1024)
    return kmeans.fit_predict(vectors)


# --- SAVE CLUSTERS ---
def save_clusters(paths, labels):
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    for p, l in zip(paths, labels):
        c.execute("UPDATE embeddings SET cluster=? WHERE path=?", (int(l), p))

    conn.commit()
    conn.close()


# --- LABEL CLUSTERS (FIXED) ---
def label_clusters():
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    c.execute("SELECT DISTINCT cluster FROM embeddings")
    clusters = [row[0] for row in c.fetchall()]

    for cid in clusters:
        c.execute("SELECT path FROM embeddings WHERE cluster=? LIMIT 10", (cid,))

        samples = [os.path.basename(row[0]) for row in c.fetchall()]

        if not samples:
            continue

        sample_text = "\n".join(samples)

        prompt = f"""
These are book titles:

{sample_text}

What is the common theme?

Answer in 3-5 words.
"""

        try:
            res = client.chat.completions.create(
                model="mixtral",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )

            label = res.choices[0].message.content.strip()
            print(f"[CLUSTER {cid}] → {label}")

        except Exception as e:
            print(f"[CLUSTER {cid}] failed: {e}")

    conn.close()


# --- MAIN ---
def main():
    init_db()

    print("[LOAD]")
    paths = load_books()

    print(f"[FOUND] {len(paths)}")

    print("[EMBED]")
    generate_embeddings(paths)

    print("[LOAD VECTORS]")
    paths, vectors = load_vectors()

    print("[CLUSTER]")
    labels = cluster_vectors(vectors)

    print("[SAVE]")
    save_clusters(paths, labels)

    print("[LABEL]")
    label_clusters()

    # ALL IT

    # At the end of main(), add:

    move_by_cluster()


if __name__ == "__main__":
    main()
