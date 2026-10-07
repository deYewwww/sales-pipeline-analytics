## Orchestration (Airflow)

Two decoupled DAGs, run locally with the Astro CLI (Airflow 3 on Docker).

| DAG | Schedule | Tasks | Role |
|---|---|---|---|
| `simulate_crm_events` | every 30 min | `generate_events` | Plays the CRM (source system): sends deal events to Kafka |
| `sales_pipeline_analytics` | hourly at :15 | `databricks_bronze_silver` → `dbt_build` | The pipeline: Kafka → Bronze → Silver (Databricks Job), then Silver → Gold (dbt) |

**Kafka is the boundary.** The pipeline never calls the simulator; it reads whatever arrived since the last checkpoint.
The 15-minute offset lets each pipeline run pick up the latest batch.

### Design decisions
- **Streaming in a batch scheduler:** Bronze uses `trigger(availableNow=True)`, so each run processes new offsets since the checkpoint and stops. Airflow tasks must finish.
- **`max_active_runs=1`:** two concurrent runs would mean two streams on one checkpoint.
- **Deferrable Databricks operator:** waiting happens in the triggerer, not a worker slot.
- **Fail loudly:** the simulator exits non-zero if any Kafka delivery fails; dbt test failures fail the task.
  If Databricks fails, `dbt_build` is `upstream_failed`, so Gold is never built from bad Silver (screenshot below).
- **Same dbt engine everywhere:** dbt Fusion 2.0.6 pinned in the Airflow image to match local development.
- **No secrets in Git:** connections, Kafka keys and the dbt token come from `orchestration/.env` (gitignored); the dbt profile reads env vars.

### Run it
1. Install Docker Desktop and the Astro CLI
2. Update the two absolute paths in `orchestration/docker-compose.override.yml` to your clone location
3. Create `orchestration/.env` with: `AIRFLOW_CONN_DATABRICKS_DEFAULT`, `AIRFLOW_VAR_DATABRICKS_JOB_ID`,
   `CONFLUENT_BOOTSTRAP_SERVERS`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET`, `KAFKA_TOPIC`,
   `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_DATABRICKS_TOKEN`
4. `cd orchestration && astro dev start`

### Known limitations / next steps
- Task retries are the same for every error; 4xx API errors should fail fast, with retries kept for 5xx and timeouts.
- Silver is rebuilt from all of Bronze each run (idempotent, simple); at scale this becomes an incremental `MERGE` on `event_id`.
- Local Docker only; the same DAGs would deploy to MWAA or Cloud Composer without code changes.