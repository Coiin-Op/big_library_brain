import pathlib
from ask import extract_text   # reuse your existing code

def process_book(path, category):

    path = pathlib.Path(path)

    text = extract_text(path)

    if not text:
        return

    snippet = text[:5000]

    # Save alongside file OR in a central DB
    meta_path = path.with_suffix(".brain.json")

    meta = {
        "file": str(path),
        "category": category,
        "snippet": snippet
    }

    import json
    meta_path.write_text(json.dumps(meta, indent=2))