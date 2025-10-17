set -eux

BIN=~/libCacheSim/_build/bin/traceAnalyzer
TRACE_LIST=~/Lazy-Promotions/scripts/current.txt
TRACE_PATH=/ltdata/data/oracleReuse
OUTPUT_DIR=~/trace_stats
THREAD=32

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

parallel -j $THREAD "$BIN" "$TRACE_PATH"/{} oracleGeneral --size :::: "$TRACE_LIST"
