#!/usr/bin/env bash
# Runs the whole Spark phase in the documented order. Stops at the first failure.
#   docker compose -f docker-compose.hadoop.yml run --rm -e HADOOP_USER_NAME=hadoop spark-runner bash run_pipeline.sh
# Local run without HDFS: SUPPLY_CHAIN_BASE=file:///path/to/lake bash run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")"
for step in read_and_validate delivery_performance seller_performance product_category payment_analysis verify_gold; do
  echo "================ $step ================"
  ${PYTHON:-python} "$step.py"
done
echo "Spark phase pipeline finished successfully."
