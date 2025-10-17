set -eux

BIN=~/libCacheSim/_build/bin/traceAnalyzer
TRACE_LIST=~/Lazy-Promotions/scripts/current.txt
TRACE_PATH=/ltdata/data/oracleReuse
OUTPUT_DIR=~/trace_stats

mkdir $OUTPUT_DIR
cd $OUTPUT_DIR

while IFS= read -r PATH; do
    $BIN $TRACE_PATH/$PATH oracleGeneral --size
done < $TRACE_LIST
