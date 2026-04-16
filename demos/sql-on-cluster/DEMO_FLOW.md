# SQL on the Cluster: Live Demo Flow

## Pre-demo setup (do before class)

1. **EC2 instance** running (t3a.xlarge, 16 GB RAM, instance profile with S3 read)
2. **EMR cluster** running (3-node m6a.xlarge, Spark + JupyterHub + Livy)
3. **Repo cloned and set up on EC2**:
   ```bash
   ssh -i ~/.ssh/vockey.pem ubuntu@<ec2-ip>
   git clone <repo-url> sql-demo && cd sql-demo
   bash setup.sh
   ```
4. **VS Code** connected via Remote SSH to EC2
5. Open `01_duckdb_s3.py`, verify kernel "SQL Demo (uv)" works

Optional:
- Upload batch script: `aws s3 cp infra/batch_user_summary.py s3://dsci525-data-2026/scripts/ --profile ilya-ubc-aws-student --region ca-central-1`
- Make data public: `bash infra/make_data_public.sh`

---

## Part 1: DuckDB on EC2 (~20 min)

### Open `01_duckdb_s3.py`

**Talking points per cell:**

| Cell | Point to make |
|------|--------------|
| Setup | 3 settings (memory, threads, temp_directory) + credential_chain. No cluster, no YARN. |
| Quick check | 207M reviews, 4 categories. DuckDB reads all partitions via glob. |
| Category summary | Low-cardinality GROUP BY: hash table has 4 entries. ~4s. Time is S3 I/O. |
| Filter + sort | Top-k optimization: DuckDB tracks top 20, never sorts all 49M rows. ~14s. |
| Cross-category join | Hive partitioning: only reads Electronics + Books partitions. Two DISTINCT sets ~50M total. ~19s. |

**Key message:** DuckDB streams 22.5 GB through 4 GB of RAM. The data never fully loads.

### Open `02_duckdb_limits.py`

| Cell | Point to make |
|------|--------------|
| GROUP BY 10M products | Hash table: 10M entries x 100 bytes = ~1 GB. Fits in 4 GB. ~52s. |
| GROUP BY + WINDOW | Two operators back to back. ~74s. Pushing the limit. |
| **Boundary discussion** | Walk through the math: 50M users x 100 bytes = 5 GB. Won't fit. |
| **OOM on user_id** | Disable spill-to-disk first. Let it fail at 4 GB. Read the error. |
| Rescue: 12 GB + spill | Re-enable spill, 12 GB, 4 threads. Succeeds in ~61s. Uses 75% of machine. |

**Key message:** The limit is not data size but intermediate structure size. DuckDB CAN handle it by scaling up, but hits three walls: memory ceiling, CPU bound, network throughput.

**Transition:** "DuckDB finishes, but the machine is near its limits. Same SQL, same data: let's distribute the work."

---

## Part 2: SparkSQL on EMR (~20 min)

### Switch VS Code Remote SSH to EMR primary node (or use terminal)

### Open `03_spark_sql.py`

| Cell | Point to make |
|------|--------------|
| Category summary | Spark overhead: ~7s vs DuckDB's ~4s. Shuffle cost for a trivial query. |
| GROUP BY 10M products | ~32s vs 52s. Distributed hash table wins. |
| GROUP BY + WINDOW | ~27s vs 74s. 2.7x faster. |
| **GROUP BY 50M users** | DuckDB did it in 61s at 75% RAM. Spark: ~32s across 3 nodes, with headroom. |
| Comparison table | Side by side. DuckDB wins small queries, Spark wins large ones. |

**While queries run:** Open Spark UI (port 4040, VS Code auto-forwards it)
- Show the Stages tab: active stage, task distribution
- Show the SQL tab: physical plan with shuffle operators
- Point out shuffle read/write bytes

### Open `04_spark_ml.py` (if time)

- SparkSQL for feature engineering -> MLlib for training
- Same cluster, same session, no intermediate files
- This is the Spark differentiator: SQL + ML in one pipeline

### Open `05_spark_fail.py`

- Intentional failures: column name typo, executor OOM
- Walk through where to find logs:
  - Spark UI (while running) -> Stages -> failed stage
  - History Server (after job) -> DAG, stage durations
  - YARN ResourceManager -> cluster-level view
  - EMR Console -> step logs for terminated clusters

---

## Part 3: Transient cluster (5 min, can be just a walkthrough)

### Show `infra/transient_step.sh`

- The batch pattern: submit script, cluster starts, runs, terminates
- No SSH, no interactive session
- `--auto-terminate` + `--steps` = fire and forget
- Show `infra/batch_user_summary.py`: the DuckDB-OOM query as a batch job

### Show `infra/create_spark_cluster.sh` for comparison

- Interactive cluster: SSH in, run notebooks, develop
- Long-running: you pay for idle time
- Transient: pay only for compute

---

## Part 4: Bruin pipeline (~10 min)

### cd into `../bruin-pipeline/` and open `run_demo.sh`

Three runs that build on each other. The script has TALK THROUGH
and EXPECT blocks for each step. File edits are done in VS Code.

| Run | What happens |
|-----|-------------|
| Run 1: full pipeline | 3 assets execute in DAG order, all quality checks pass, query result |
| Run 2: break a check | Remove grade 'e' from accepted_values. Check fails, downstream blocked |
| Run 3: fix + update | Restore 'e', add max_sugar column. Clean result with new column |

**Key messages:**

1. **DAG resolution**: Bruin reads `depends:` and runs assets in the right order
2. **Quality gates**: checks run after each asset. Failure stops downstream assets.
3. **Error propagation prevention**: bad data caught at the source, not 3 steps later
4. **Full refresh**: every `bruin run` re-executes everything from scratch
5. **bruin query**: reads existing tables without re-running the pipeline

**Transition:** "This is what happens when your one-time scripts become recurring.
A pipeline tool handles ordering, validation, and failure propagation for you."

---

## Stretch: Trino (~5 min, if time)

### Show `infra/create_trino_cluster.sh`

- Dedicated cluster (no Spark): Trino runs outside YARN
- trino-cli on primary node
- Same query, third engine
- Pipeline execution: starts returning results before full scan

---

## Memory estimation for the boundary discussion

```
Hash table entry for GROUP BY:
  Key (string ~10 chars):     ~26 bytes
  Hash value:                    8 bytes
  COUNT(*):                      8 bytes
  SUM(rating) + COUNT for AVG:  16 bytes
  Pointer / metadata:           16 bytes
  ─────────────────────────────────────
  Total per entry (aligned):   ~100 bytes

10M products x 100 bytes = ~1 GB   -> fits in 4 GB (with 4 threads)
50M users    x 100 bytes = ~5 GB   -> OOM at 4 GB

With 4 thread-local partitions:
  10M / 4 = 2.5M entries/thread x 100 B = 250 MB/thread -> OK
  50M / 4 = 12.5M entries/thread x 100 B = 1.25 GB/thread
  4 threads x 1.25 GB = 5 GB minimum -> exceeds 4 GB limit
```
