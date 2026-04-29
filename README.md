# Cortex Loop Demo

Minimal repo for testing the Cortex Databricks experiment loop. Computes the mean of a list of numbers via PySpark, writes a metrics JSON and a bar chart figure to a Cortex output directory. Designed to be just complex enough to exercise the full loop (parameter passing, Spark session, artifact writing, structured stdout result) while still finishing in well under a minute.

## Project Structure

```
cortex-loop-demo/
├── README.md
├── requirements.txt
├── .cortex-project.yaml
├── .gitignore
└── src/
    └── train.py
```

## Standard Cortex Conventions

- Reads `CORTEX_RUN_ID` and `CORTEX_OUTPUT_DIR` from environment variables OR from `sys.argv` (env vars take precedence; falls back to argv so the Databricks helper can pass them as job parameters).
- Prints a single final line of the form `CORTEX_RESULT: {json}` so the loop driver can parse status, metrics, and artifact paths from stdout.
- Writes all artifacts (metrics, figures, checkpoints) under `$CORTEX_OUTPUT_DIR/` so the loop can collect them after the run.
- Returns non-zero exit status on failure and emits a `CORTEX_RESULT` line with `"status": "fail"` so failures are still machine-readable.

## How to Run

### 1. Locally (for development)

```bash
pip install -r requirements.txt
export CORTEX_RUN_ID=local-dev-001
export CORTEX_OUTPUT_DIR=./local_outputs/local-dev-001
mkdir -p "$CORTEX_OUTPUT_DIR"
python3 src/train.py --values 1,2,3
```

### 2. Via Databricks Jobs API (one-shot submit)

```bash
databricks jobs submit --json '{
  "run_name": "cortex-loop-demo-001",
  "existing_cluster_id": "0325-230746-e3qf979u",
  "spark_python_task": {
    "python_file": "file:///Workspace/Repos/<user>/cortex-loop-demo/src/train.py",
    "parameters": ["--values", "1,2,3",
                   "--cortex-run-id", "loop-001",
                   "--cortex-output-dir", "/dbfs/cortex/runs/loop-001"]
  }
}'
```

### 3. Via the Cortex `db-run-script.sh` helper

```bash
./scripts/db-run-script.sh \
  --repo cortex-loop-demo \
  --entry src/train.py \
  --run-id loop-001 \
  --params "--values 1,2,3"
```

The helper handles syncing the repo to Databricks, submitting the job, polling for completion, parsing the `CORTEX_RESULT` line, and downloading artifacts from `$CORTEX_OUTPUT_DIR`.

## Expected Output

stdout will end with:

```
CORTEX_RESULT: {"status": "ok", "metric": 2.0, "message": "Computed mean of [1,2,3]", "artifacts": ["metrics.json", "figures/bar.png"]}
```

Artifacts:

- `$CORTEX_OUTPUT_DIR/metrics.json` — `{"mean": 2.0, "count": 3, "values": [1, 2, 3]}`
- `$CORTEX_OUTPUT_DIR/figures/bar.png` — a simple bar chart of `[1, 2, 3]` titled "Demo Values".

## License

MIT
