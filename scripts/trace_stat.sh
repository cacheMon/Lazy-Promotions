set -eux

BIN=/users/muhhae/libCacheSim/_build/bin/traceAnalyzer
TRACE_LIST=./current.txt
TRACE_PATH=/ltdata/data/oracleReuse
OUTPUT_DIR=/users/muhhae/trace_stats

echo > ~/task

while IFS= read -r PATH; do
    FILE=$TRACE_PATH/$PATH

    size=$(stat --format="%s" "$FILE")
    gb=$(( (size + 1024*1024*1024 - 1) / (1024*1024*1024) ))
    min_dram=$(( gb/4+1 ))
    priority=$(( 100/gb + 1 ))

    echo "shell:$priority:$min_dram:1: mkdir -p $OUTPUT_DIR && cd $OUTPUT_DIR && $BIN $FILE oracleGeneral --size" >> ~/task
done < $TRACE_LIST
