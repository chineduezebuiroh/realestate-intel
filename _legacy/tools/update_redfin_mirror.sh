#!/usr/bin/env bash
set -euo pipefail

# --- Config you set once ---
MIRROR_REPO_SSH="${MIRROR_REPO_SSH:-git@github.com:YOUR_GH_USERNAME/market-data-mirror.git}"
MIRROR_REPO_DIR="${MIRROR_REPO_DIR:-../market-data-mirror}"   # sibling folder by default
MIRROR_SUBDIR="${MIRROR_SUBDIR:-redfin}"                      # where to put the csv in mirror repo
RAW_NAME="${RAW_NAME:-weekly_market_totals.csv}"

# --- Paths in your main repo ---
MAIN_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.."; pwd)"
RAW_DIR="$MAIN_ROOT/data/raw/redfin"
RAW_FILE="$RAW_DIR/$RAW_NAME"

echo "[mirror] working from: $MAIN_ROOT"
echo "[mirror] target repo dir: $MIRROR_REPO_DIR"

# 0) Ensure Redfin CSV exists locally (runs your resilient ingest if needed)
if [[ ! -f "$RAW_FILE" ]]; then
  echo "[mirror] local Redfin CSV missing — running ingest..."
  (cd "$MAIN_ROOT" && source .venv/bin/activate && python ingest/redfin_market_trends.py)
else
  echo "[mirror] found existing Redfin CSV: $RAW_FILE"
fi

if [[ ! -f "$RAW_FILE" ]]; then
  echo "[mirror] ERROR: Redfin CSV still missing after ingest. Aborting."
  exit 1
fi

# 1) Clone or pull the mirror repo
if [[ -d "$MIRROR_REPO_DIR/.git" ]]; then
  echo "[mirror] updating existing mirror repo..."
  (cd "$MIRROR_REPO_DIR" && git fetch origin && git pull --rebase --autostash)
else
  echo "[mirror] cloning mirror repo..."
  git clone "$MIRROR_REPO_SSH" "$MIRROR_REPO_DIR"
fi

# 2) Copy the CSV into mirror repo path
mkdir -p "$MIRROR_REPO_DIR/$MIRROR_SUBDIR"
cp "$RAW_FILE" "$MIRROR_REPO_DIR/$MIRROR_SUBDIR/$RAW_NAME"

# 3) Commit only if changed
cd "$MIRROR_REPO_DIR"
git add "$MIRROR_SUBDIR/$RAW_NAME"

if git diff --cached --quiet; then
  echo "[mirror] no changes to commit — mirror already up to date."
else
  MSG="mirror: update $RAW_NAME ($(date -u +'%Y-%m-%dT%H:%M:%SZ'))"
  git commit -m "$MSG"
  git push origin HEAD:main
  echo "[mirror] pushed update → $(git remote get-url origin)"
fi

# 4) Print the RAW URL you should put in REDFIN_MIRRORS
USER_REPO="$(git remote get-url origin | sed -E 's#.*github.com[:/](.+/.+)\.git#\1#')"
RAW_URL="https://raw.githubusercontent.com/${USER_REPO}/main/${MIRROR_SUBDIR}/${RAW_NAME}"
echo "[mirror] RAW URL (set this in REDFIN_MIRRORS secret):"
echo "$RAW_URL"
