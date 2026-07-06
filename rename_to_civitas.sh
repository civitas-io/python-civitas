#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/kodiak/workspace/Projects/python-agency"
cd "$ROOT"

echo "=== Phase 3: Bulk rename agency → civitas ==="

FILES=$(find . \
  -type f \
  \( -name "*.py" -o -name "*.tmpl" -o -name "*.yaml" -o -name "*.yml" \) \
  ! -path "./.git/*" \
  ! -path "./.venv/*" \
  ! -path "./__pycache__/*")

for f in $FILES; do
  # Import paths
  sed -i 's/from agency\./from civitas./g'       "$f"
  sed -i 's/from agency import/from civitas import/g' "$f"
  sed -i 's/import agency\./import civitas./g'   "$f"
  sed -i 's/^import agency$/import civitas/g'    "$f"

  # Package install name
  sed -i 's/python-agency/civitas/g'             "$f"
  sed -i 's/python_agency/python_civitas/g'      "$f"

  # CLI command references in comments and docstrings
  sed -i 's/agency run/civitas run/g'            "$f"
  sed -i 's/agency deploy/civitas deploy/g'      "$f"
  sed -i 's/agency init/civitas init/g'          "$f"
  sed -i 's/agency --/civitas --/g'              "$f"

  # String literals (error messages, version banners, install hints)
  sed -i 's/"agency"/"civitas"/g'                "$f"
  sed -i "s/'agency'/'civitas'/g"                "$f"

  # Class name
  sed -i 's/AgencyError/CivitasError/g'          "$f"

  # Prose in docstrings/comments: "Agency" as product name
  sed -i 's/# Agency/# Civitas/g'               "$f"
done

echo "=== Done ==="
