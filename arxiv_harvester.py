
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import os
import time
import concurrent.futures

# ---------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------

BASE_DIR = r"D:\Downloads\Downloaded\_research_library"
LOG_FILE = os.path.join(BASE_DIR, "downloaded_ids.txt")

RESULTS_PER_PAGE = 100
MAX_PAPERS_PER_TOPIC = 2000
DOWNLOAD_THREADS = 6
REQUEST_DELAY = 1.5

SEARCH_TOPICS = {
    "orbital_dynamics": "all:orbital dynamics",
    "orbital_mechanics": "all:orbital mechanics",
    "lambert_problem": "all:Lambert problem",
    "fluid_dynamics": "cat:physics.flu-dyn",
    "computational_fluid_dynamics": "all:computational fluid dynamics",
    "control_systems": "cat:cs.SY",
    "robotics_navigation": "cat:cs.RO"
}

# ---------------------------------------------------------------
# Ignore proxy settings (prevents Tor / SOCKS issues)
# ---------------------------------------------------------------

for k in ["HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy"]:
    os.environ.pop(k, None)

os.makedirs(BASE_DIR, exist_ok=True)

# ---------------------------------------------------------------
# Load downloaded IDs
# ---------------------------------------------------------------

downloaded_ids = set()

if os.path.exists(LOG_FILE):
    with open(LOG_FILE) as f:
        downloaded_ids = set(line.strip() for line in f)

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def safe_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-")[:120]


def fetch_url(url):

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    with urllib.request.urlopen(req) as r:
        return r.read()


def download_pdf(task):

    url, filepath, arxiv_id, title = task

    try:

        if os.path.exists(filepath):
            return

        print("Downloading:", title)

        data = fetch_url(url)

        with open(filepath, "wb") as f:
            f.write(data)

        with open(LOG_FILE, "a") as f:
            f.write(arxiv_id + "\n")

    except Exception as e:
        print("Download failed:", e)


# ---------------------------------------------------------------
# Harvest topic
# ---------------------------------------------------------------

def harvest_topic(topic_name, query):

    print("\n===== Harvesting:", topic_name, "=====")

    topic_dir = os.path.join(BASE_DIR, topic_name)
    os.makedirs(topic_dir, exist_ok=True)

    start = 0
    encoded_query = urllib.parse.quote(query)

    while start < MAX_PAPERS_PER_TOPIC:

        url = (
            "http://export.arxiv.org/api/query?"
            f"search_query={encoded_query}&start={start}&max_results={RESULTS_PER_PAGE}"
        )

        print("Query:", url)

        data = fetch_url(url)

        root = ET.fromstring(data)

        entries = root.findall("{http://www.w3.org/2005/Atom}entry")

        if not entries:
            break

        tasks = []

        for entry in entries:

            arxiv_id = entry.find("{http://www.w3.org/2005/Atom}id").text.split("/")[-1]

            if arxiv_id in downloaded_ids:
                continue

            downloaded_ids.add(arxiv_id)

            title = entry.find("{http://www.w3.org/2005/Atom}title").text.strip()

            pdf_url = None

            for link in entry.findall("{http://www.w3.org/2005/Atom}link"):
                if link.attrib.get("title") == "pdf":
                    pdf_url = link.attrib["href"]

            if pdf_url:

                filename = safe_filename(title) + ".pdf"
                filepath = os.path.join(topic_dir, filename)

                tasks.append((pdf_url, filepath, arxiv_id, title))

        # Download in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS) as executor:
            executor.map(download_pdf, tasks)

        time.sleep(REQUEST_DELAY)

        start += RESULTS_PER_PAGE


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

for topic, query in SEARCH_TOPICS.items():
    harvest_topic(topic, query)

print("\nHarvest complete.")
