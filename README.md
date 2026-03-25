This is a project to catalogue and sort a 3.4 million file personal text library.

I built it to solve a real problem: organizing a massive, unstructured collection without having to manually sort everything.

It automatically:
- scans large directories
- extracts snippets from files
- generates embeddings for classification
- uses LLMs for fallback categorization
- detects duplicates
- organizes files into structured folders

Designed for large-scale libraries (100k+ files) where manual sorting is not practical.

I’m sharing this to help the next person trying to clean up a massive, unorganized library so they don’t have to repeat the same trial-and-error process.

## Requirements
- Python 3.10+
- LM Studio (or compatible local API)

## How to run
1. Configure paths in the script
2. Start LM Studio
3. Run: python scan_and_sort_main.py
