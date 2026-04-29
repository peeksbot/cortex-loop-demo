#!/usr/bin/env python3
"""Cortex loop demo entry point.

Computes the mean of [1,2,3] using PySpark, writes metrics.json and values.csv
to the Cortex output directory, prints a single CORTEX_RESULT line.

Note: this script does NOT call sys.exit(). Databricks' spark_python_task
runs scripts inside an IPython kernel which treats sys.exit() as a SystemExit
exception and marks the run as FAILED. Just let the script return naturally.
"""

import json
import os
import sys


# Helper passes [run_id, output_dir] as the first two positional args.
run_id = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CORTEX_RUN_ID", "local-dev")
output_dir = (
    sys.argv[2]
    if len(sys.argv) > 2
    else os.environ.get("CORTEX_OUTPUT_DIR", "./local_outputs/local-dev")
)

try:
    os.makedirs(output_dir, exist_ok=True)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("cortex-loop-demo").getOrCreate()

    values = [1, 2, 3]
    df = spark.createDataFrame([(v,) for v in values], ["value"])
    mean_val = float(df.agg({"value": "avg"}).collect()[0][0])
    count_val = df.count()

    with open(os.path.join(output_dir, "metrics.json"), "w") as f:
        json.dump({"mean": mean_val, "count": count_val, "values": values, "run_id": run_id}, f)

    with open(os.path.join(output_dir, "values.csv"), "w") as f:
        f.write("idx,value\n")
        for i, v in enumerate(values):
            f.write(f"{i},{v}\n")

    result = {
        "status": "ok",
        "metric": mean_val,
        "message": f"Computed mean of {values}",
        "artifacts": ["metrics.json", "values.csv"],
    }
    print(f"CORTEX_RESULT: {json.dumps(result)}")

except Exception as e:
    # Don't sys.exit on failure either — raise so Databricks marks task FAILED
    # but our CORTEX_RESULT line is still captured.
    print(f"CORTEX_RESULT: {json.dumps({'status': 'fail', 'message': str(e), 'artifacts': []})}")
    raise
