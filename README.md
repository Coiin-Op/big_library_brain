# Large-Scale Library Sorter & AI Query System

A high-throughput, multi-model AI pipeline designed to organize and query massive unstructured text libraries.

Built to solve a real problem: automatically sorting a 3.4 million file personal library without manual effort.

---

## 🚀 Overview

This system scans large directories, classifies files using embeddings + LLMs, and organizes them into structured categories.

It is designed for scale, efficiency, and reliability.

---

## 🔥 Core Features

- ⚡ High-throughput async processing
- 🧠 Embedding-based classification
- 🤖 Multi-model LLM routing
- 🔁 Streaming pipeline (not batch)
- 🧬 Duplicate detection (semantic)
- 📊 Real-time progress + ETA
- 💾 Disk-safe execution mode
- 🔍 Observability + debug visibility
- ⚙️ Resource-aware (CPU / RAM / VRAM)
- 🧠 Hybrid system (heuristics + AI)
- 🔁 Resume-safe execution

---

## 🧠 System Design

### Intelligent Pipeline
- Embedding similarity → fast classification
- LLM fallback for uncertain cases
- Threshold-based decision routing

### Cost-Aware Architecture
- Avoids unnecessary LLM calls
- Uses smaller models when possible

### Modular AI Orchestration
- Swap models without changing logic
- Supports multiple local LLMs

### Parallel Execution
- Async processing with controlled concurrency
- Optimized for large datasets

---

## ⚙️ What It Does

- Scans large directories
- Extracts text snippets (start, middle, end)
- Generates embeddings
- Detects duplicates
- Classifies files into categories
- Moves files into structured folders
- Uses local LLMs for edge cases

---

## 📦 Scale

Designed for:

- 100k+ files (minimum)
- Tested with 3.4 million files
- Handles large datasets efficiently

---

## 🧪 Requirements

- Python 3.10+
- LM Studio (or compatible local API)
- Local embedding model

---

## ▶️ How to Run

1. Configure paths in the script
2. Start LM Studio
3. Run:

```bash
python scan_and_sort_main_v3.0.py

## 🧠 System Architecture

```mermaid

flowchart TD

    A[📥 Data Sources<br/>3.4M Files] --> B[⚡ Async Scanner]

    B --> C[🧠 Snippet Extraction<br/>(start/middle/end)]
    C --> D[🔢 Embeddings]

    D --> E{🔍 Similarity Check}

    E -->|High Confidence| F[📂 Direct Classification]
    E -->|Low Confidence| G[🤖 LLM Routing]

    G --> H[⚡ Fast Model (Qwen)]
    G --> I[🪶 Small Model (Phi)]

    H --> J[📁 Category Assigned]
    I --> J

    F --> J

    J --> K[📦 Move File to Folder]

    K --> L[🔁 Organized Library]

    L --> M[🔍 Recoll Index]

    M --> N[📊 Search Results]

    N --> O[🧠 Context Builder]

    O --> P[🤖 AI Reasoning]

    P --> Q[💬 Answer + Sources]
```
### Flow Summary

1. Files are scanned and embedded  
2. High-confidence matches are auto-classified  
3. Low-confidence files are routed to LLMs  
4. Files are organized into structured folders  
5. Recoll indexes the library  
6. Frontend retrieves + ranks results  
7. AI generates answers using real context  
