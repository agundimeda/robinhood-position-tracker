#!/usr/bin/env bash
# Cron entry point: snapshot every weekday; post Positions then Trades (two messages) on a change day, else Positions on post_weekday (0=Mon .. 4=Fri).
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
cd "$(dirname "$0")"
log="logs/$(date +%F).log"
{
  echo "=== $(date -Is) snapshot"
  python3 tracker.py snapshot \
    || { sleep 120; echo "retry 2 (sonnet)"; python3 tracker.py snapshot; } \
    || { sleep 120; echo "retry 3 (opus)"; TRACKER_MODEL=claude-opus-5 python3 tracker.py snapshot; }
  echo "=== $(date -Is) daily post"
  python3 tracker.py daily || echo "post failed (snapshot is stored; rerun: tracker.py daily)"
} >>"$log" 2>&1
