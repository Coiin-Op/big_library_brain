from sorterbook_sorter_v9 import sort_books   # your existing sorter function
from brain_ingest import process_book
import pathlib

LIBRARY_PATH = r"D:\Books"  # adjust this

def run_pipeline():

    print("=== STEP 1: SORTING BOOKS ===")
    sorted_books = sort_books(LIBRARY_PATH)

    print("=== STEP 2: FEEDING BRAIN ===")

    for book in sorted_books:
        try:
            process_book(book["path"], book["category"])
        except Exception as e:
            print("Brain ingest failed:", e)

    print("=== DONE ===")


if __name__ == "__main__":
    run_pipeline()