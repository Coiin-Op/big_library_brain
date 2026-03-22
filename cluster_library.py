import subprocess
import pathlib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans


def search_recoll(query, max_results=200):

    cmd = [r"D:\recoll\recollq.exe", "-t", "-n", str(max_results), query]

    result = subprocess.run(cmd, capture_output=True, text=True)

    files = []

    for line in result.stdout.splitlines():

        if "file:///" in line:

            path = line.split("file:///")[1].split("]")[0]

            p = pathlib.Path(path)

            if p.exists():
                files.append(p)

    return files


def read_text(path):

    try:
        return path.read_text(errors="ignore")[:2000]
    except:
        return ""


query = input("Search topic: ")

results = search_recoll(query)

documents = []
names = []

for r in results[:50]:

    text = read_text(r)

    if text:

        documents.append(text)

        names.append(r.name)


vectorizer = TfidfVectorizer(stop_words="english")

X = vectorizer.fit_transform(documents)

kmeans = KMeans(n_clusters=5, random_state=42)

labels = kmeans.fit_predict(X)

clusters = {}

for i, label in enumerate(labels):

    clusters.setdefault(label, []).append(names[i])


print("\n--- CLUSTERS ---\n")

for c in clusters:

    print(f"\nCluster {c}\n")

    for doc in clusters[c][:10]:
        print(" ", doc)
