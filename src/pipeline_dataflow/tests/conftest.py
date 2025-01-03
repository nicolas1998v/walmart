import os
import sys
from pathlib import Path

# Get absolute paths
root_dir = Path(__file__).parent.parent
src_dir = root_dir / "src"

# Add paths to sys.path if they're not already there
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# Debug print
print("\nPython paths:")
print("\n".join(sys.path))
