#!/usr/bin/env python3
"""Remove all history entries where gemini_category is 'error' or 'unknown'."""

import json
import os

HISTORY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.json")

def main():
    if not os.path.exists(HISTORY_PATH):
        print("history.json not found")
        return

    with open(HISTORY_PATH, "r") as f:
        history = json.load(f)

    items = history.get("items", {})
    to_remove = [
        handle for handle, entry in items.items()
        if entry.get("gemini_category") in ("error", "unknown")
    ]

    for handle in to_remove:
        del items[handle]

    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)

    print(f"Removed {len(to_remove)} entries (error/unknown). {len(items)} entries remaining.")


if __name__ == "__main__":
    main()
