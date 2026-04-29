#!/usr/bin/env python3
"""Cortex loop demo entry point.

Computes the mean of [1,2,3] using PySpark, writes metrics.json and values.csv
to the Cortex output directory, prints a single CORTEX_RESULT line.
"""

import json
import os
import sys


def main() -> int:
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

        # Write artifacts
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
        return 0

    except Exception as e:
        print(f"CORTEX_RESULT: {json.dumps({'status': 'fail', 'message': str(e), 'artifacts': []})}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
