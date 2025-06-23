#!/usr/bin/env python3
# test_path.py
import os
from pathlib import Path

# The exact line of code you want to test
db_path = Path(__file__).parent.parent / "baseball_history.db"

# Print the Path object as it's constructed
print(f"Constructed Path: {db_path}")

# Print the fully resolved, absolute path
# .resolve() makes it canonical (e.g., handles '..') and absolute
print(f"Resolved Path:    {db_path.resolve()}")

# You can also check if the file actually exists at that path
print(f"Does file exist?  {db_path.exists()}")
