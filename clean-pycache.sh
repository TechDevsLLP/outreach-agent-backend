#!/usr/bin/env bash
# Delete all __pycache__ directories and .pyc/.pyo files under the backend.
# Usage: ./clean-pycache.sh
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Cleaning Python caches under: $DIR"

# Count before removing so we can report.
pycache_count=$(find "$DIR" -type d -name '__pycache__' | wc -l | tr -d ' ')
pyc_count=$(find "$DIR" -type f \( -name '*.pyc' -o -name '*.pyo' \) | wc -l | tr -d ' ')

find "$DIR" -type d -name '__pycache__' -prune -exec rm -rf {} +
find "$DIR" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

echo "Removed $pycache_count __pycache__ dir(s) and $pyc_count compiled file(s)."
