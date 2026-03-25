# Large-Scale Library Sorter & AI Query System

This project was built to catalogue and sort a 3.4 million file personal text library.

I built it to solve a real problem: organizing a massive, unstructured collection without having to manually sort everything.

## Features
Its a A High-throughput, multi-model, async AI classification pipeline
with heuristic routing and real-time observability


🧠 1. Intelligent decision pipeline
⚡ 2. Cost-aware architecture
🧩 3. Modular AI orchestration
🔁 4. Streaming pipeline (not batch)
🧠 5. Heuristic + AI hybrid system
🔍 6. Observability
⚡ 7. Parallel async execution
🧬 8. Deduplication engine
🧠 9. Self-improving system (partially there)
🔥 10. Resource-aware engineering 
🚀 11. scaling workload 
🔥 12. Multi-model routing
⚡ 13. Concurrency tuned
📊 14. Progress + ETA
💾 15. Disk-safe mode
🧠 16. Embedding + duplicate detection

It automatically:
- scans large directories
- extracts snippets from files (start, middle, end for better accuracy)
- generates embeddings for classification
- uses LLMs for fallback categorization
- organizes files into structured folders
- Async + batched processing
- Cost-optimized AI usage
- Duplicate detection
- Resume-safe execution
- Local LLM + embeddings support

Designed for large-scale libraries (100k+ files) where manual sorting is not practical.

---

## Main Script

Use this file to run the sorter:

- `scan_and_sort_main_v2.5`  ← **current main version**

ChatGPT was used extensively during this project for debugging, design iteration, and refining the overall pipeline.

## Script Evolution (for reference / experimentation)

These are older or experimental versions if you want to explore or modify behavior:

- book_sorter_v3 (earliest saved version)
- book_sorter_v9 (best of first-gen scripts)
- scan_and_sort (early version)
- scan_and_sort_main (first solid version)
- scan_and_sort_main_v1.5
- scan_and_sort_main_v2.0
- scan_and_sort_main_anaconda (Anaconda version)
- scan_and_sort_main_co_pilot (AI-assisted version)

Experimental / test builds:
- mad_scientist_embeddings
- mad_scientist_embeddings_v2
- run_full_pipeline (early full pipeline attempt)

CoPilot variants:
- CoPilot_scan_and_sort
- CoPilot_scan_and_sort_2

---

## Requirements

- Python 3.10+
- LM Studio (or compatible local API)

---

## How to Run (Sorter)

1. Configure paths in the script
2. Start LM Studio
3. Run:

```bash
python scan_and_sort_main_v2.5.py
