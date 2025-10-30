#!/usr/bin/env bash
set -euo pipefail

MODE=${MODE:-wal}
ROWS=${ROWS:-4096}
BATCHES=${BATCHES:-64}
DENSITY=${DENSITY:-0.2}
SYNC=${SYNC:-disabled}
COLUMNS=${COLUMNS:-2}

cargo run --example wal_bench -- \
  --mode=$MODE \
  --rows=$ROWS \
  --batches=$BATCHES \
  --density=$DENSITY \
  --sync=$SYNC \
  --columns=$COLUMNS
