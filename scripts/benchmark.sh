#!/bin/bash

# Runs the pipeline at a given population size and records timing + memory

# for each stage. Usage: ./scripts/benchmark.sh <population_size>



set -e



POP="${1:-100}"

DATE=$(date +%Y-%m-%d)-bench-$POP

RESULTS_FILE="benchmark_results.csv"



if [ ! -f "$RESULTS_FILE" ]; then

    echo "population,stage,seconds,peak_memory_kb" > "$RESULTS_FILE"

fi



echo "=== Benchmarking population size: $POP ==="



# Generate data

cd tools/synthea

/usr/bin/time -f "%e %M" -o /tmp/_bench.txt ./run_synthea -p "$POP" > /dev/null 2>&1

read -r secs mem < /tmp/_bench.txt

echo "$POP,synthea_generate,$secs,$mem" >> "../../$RESULTS_FILE"

cd ../..



# Ingestion

/usr/bin/time -f "%e %M" -o /tmp/_bench.txt python3 -m ingestion.load_to_hdfs --date "$DATE" > /dev/null

read -r secs mem < /tmp/_bench.txt

echo "$POP,ingestion,$secs,$mem" >> "$RESULTS_FILE"



# # ETL

/usr/bin/time -f "%e %M" -o /tmp/_bench.txt python3 -m etl.validate_and_clean --date "$DATE" > /dev/null

read -r secs mem < /tmp/_bench.txt

echo "$POP,etl,$secs,$mem" >> "$RESULTS_FILE"



# Aggregates

 /usr/bin/time -f "%e %M" -o /tmp/_bench.txt python3 -m analytics.aggregate --date "$DATE" > /dev/null
read -r secs mem < /tmp/_bench.txt

echo "$POP,aggregates,$secs,$mem" >> "$RESULTS_FILE"



echo "=== Done. Results appended to $RESULTS_FILE ==="