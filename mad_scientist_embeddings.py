import os
import sqlite3
import json
import time
import numpy as np
from openai import OpenAI
from sklearn.cluster import MiniBatchKMeans

# --- CONFIG ---
DB_PATH = "classification_cache.db"
EMBED_DB = "embeddings.db"
MODEL = "nomic-embed-text"  # change if needed
BATCH_SIZE = 32
NUM_CLUSTERS = 50

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")


# --- DB SETUP ---
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


# --- LOAD BOOKS ---
def load_books():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT path FROM cache")
    paths = [row[0] for row in c.fetchall()]
    conn.close()
    return paths


# --- GET TEXT PREVIEW ---
def get_preview(path, max_chars=800):
    try:
        with open(path, "r", errors="ignore") as f:
            return f.read(max_chars)
    except:
        return ""


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


# --- GENERATE EMBEDDINGS ---
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

            print(f"[EMBED] {len(batch)} processed")

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
    print("[CLUSTER] Running MiniBatchKMeans...")
    kmeans = MiniBatchKMeans(n_clusters=NUM_CLUSTERS, batch_size=1024)
    labels = kmeans.fit_predict(vectors)
    return labels


# --- SAVE CLUSTERS ---
def save_clusters(paths, labels):
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    for p, l in zip(paths, labels):
        c.execute("UPDATE embeddings SET cluster=? WHERE path=?", (int(l), p))

    conn.commit()
    conn.close()


# --- LABEL CLUSTERS WITH AI ---
def label_clusters():
    conn = sqlite3.connect(EMBED_DB)
    c = conn.cursor()

    c.execute("SELECT DISTINCT cluster FROM embeddings")
    clusters = [row[0] for row in c.fetchall()]

    for cluster_id in clusters:
        c.execute("SELECT path FROM embeddings WHERE cluster=? LIMIT 10", (cluster_id,))
        samples = [row[0] for row in c.fetchall()]

        sample_text = "\n".join(samples)

        prompt = f"""
These are book file names:

{sample_text}

What is the common theme or genre?

Answer in 3-5 words.
"""

        try:
            res = client.chat.completions.create(
                model="mixtral",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )

            label = res.choices[0].message.content.strip()
            print(f"[CLUSTER {cluster_id}] → {label}")

        except:
            print(f"[CLUSTER {cluster_id}] → labeling failed")

    conn.close()


# --- MAIN ---
def main():
    init_db()

    print("[LOAD] Getting books...")
    paths = load_books()

    print(f"[INFO] {len(paths)} books found")

    print("[STEP 1] Generating embeddings...")
    generate_embeddings(paths)

    print("[STEP 2] Loading vectors...")
    paths, vectors = load_vectors()

    print("[STEP 3] Clustering...")
    labels = cluster_vectors(vectors)

    print("[STEP 4] Saving clusters...")
    save_clusters(paths, labels)

    print("[STEP 5] Labeling clusters...")
    label_clusters()


if __name__ == "__main__":
    main()
