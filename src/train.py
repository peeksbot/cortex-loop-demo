#!/usr/bin/env python3
"""Cortex loop demo entry point.

Computes the mean of a small list of numbers using PySpark, writes a
``metrics.json`` and a bar-chart PNG to ``$CORTEX_OUTPUT_DIR``, and prints a
single ``CORTEX_RESULT: {...}`` line on stdout for the loop driver to parse.

Designed to run unmodified on a Databricks cluster (where Spark is preinstalled)
or locally (where ``SparkSession.builder.getOrCreate()`` spins up a local
session).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: must come before pyplot import
import matplotlib.pyplot as plt  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def _resolve_param(env_key: str, argv_flag: str, default: str | None = None) -> str | None:
    """Resolve a Cortex parameter from env first, then sys.argv, then default."""
    val = os.environ.get(env_key)
    if val:
        return val
    if argv_flag in sys.argv:
        idx = sys.argv.index(argv_flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cortex loop demo trainer")
    parser.add_argument(
        "--values",
        type=str,
        default="1,2,3",
        help="Comma-separated integers to average (default: 1,2,3)",
    )
    # Cortex params accepted as CLI fallbacks (env vars take precedence). We
    # parse them with parse_known_args so they don't conflict with our manual
    # _resolve_param scan.
    parser.add_argument("--cortex-run-id", dest="cortex_run_id", default=None)
    parser.add_argument("--cortex-output-dir", dest="cortex_output_dir", default=None)
    args, _ = parser.parse_known_args()
    return args


def main() -> int:
    args = parse_args()

    run_id = _resolve_param("CORTEX_RUN_ID", "--cortex-run-id", default="local-dev")
    output_dir_str = _resolve_param(
        "CORTEX_OUTPUT_DIR", "--cortex-output-dir", default="./local_outputs/local-dev"
    )
    output_dir = Path(output_dir_str)

    try:
        values = [int(v.strip()) for v in args.values.split(",") if v.strip()]
        if not values:
            raise ValueError("--values must contain at least one integer")

        output_dir.mkdir(parents=True, exist_ok=True)
        figures_dir = output_dir / "figures"
        os.makedirs(figures_dir, exist_ok=True)

        spark = (
            SparkSession.builder.appName("cortex-loop-demo")
            .getOrCreate()
        )

        df = spark.createDataFrame([(i, v) for i, v in enumerate(values)], ["idx", "value"])
        agg_row = df.agg(F.avg("value").alias("mean"), F.count("value").alias("count")).collect()[0]
        mean_val = float(agg_row["mean"])
        count_val = int(agg_row["count"])

        metrics = {"mean": mean_val, "count": count_val, "values": values}
        metrics_path = output_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2))

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(range(len(values)), values, color="#4C72B0")
        ax.set_title("Demo Values")
        ax.set_xlabel("Index")
        ax.set_ylabel("Value")
        ax.set_xticks(range(len(values)))
        fig.tight_layout()
        fig_path = figures_dir / "bar.png"
        fig.savefig(fig_path, dpi=120)
        plt.close(fig)

        spark.stop()

        result = {
            "status": "ok",
            "metric": mean_val,
            "message": f"Computed mean of {values}",
            "artifacts": ["metrics.json", "figures/bar.png"],
        }
        print(f"CORTEX_RESULT: {json.dumps(result)}")
        print(f"[cortex-loop-demo] run_id={run_id} output_dir={output_dir}", file=sys.stderr)
        return 0

    except Exception as e:  # noqa: BLE001 — we want to catch anything for the loop
        fail = {"status": "fail", "message": str(e), "artifacts": []}
        print(f"CORTEX_RESULT: {json.dumps(fail)}")
        print(f"[cortex-loop-demo] FAILED run_id={run_id}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
