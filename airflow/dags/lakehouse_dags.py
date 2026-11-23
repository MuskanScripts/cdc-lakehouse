"""Week 5 DAGs (ADR-029). How the work is split:

  30 s gold loop (dbt container)  build -> DQ gate -> publish, for dashboard freshness. Not Airflow:
                                  a 30 s schedule plus task start-up would cost more than the build.
  dbt_run         every 5 min     the SAME cycle (cycle.sh, flock'd with the loop) so every gold
                                  build has an auditable run, a task status and an alert on failure.
  dq_checks       every 5 min     the DQ suite on SILVER: quarantines bad records, alerts.
  optimize        nightly 02:00   streaming paused -> OPTIMIZE ZORDER + VACUUM 7 days -> resumed.
  backfill_silver manual, params  streaming paused -> rebuild a silver table from bronze -> resumed.

The rule "gold is published only if DQ passes" is enforced inside cycle.sh, so it holds for the
loop, for Airflow, and when Airflow is not running at all.
"""
from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

import lakehouse_lib as lib

TABLES = ["users", "products", "orders", "order_items", "payments"]
START = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
DEFAULTS = {"owner": "lakehouse", "on_failure_callback": lib.on_failure, "retries": 0}


# ------------------------------------------------------------------ dq_checks

def run_dq_checks(**_):
    rc, out = lib.exec_in("dbt", ["/opt/soda/bin/python", "/quality/dq.py", "--source", "silver", "--quarantine"])
    summary = lib.last_line(out, "DQ ")
    if rc == 2:
        raise AirflowException(f"silver failed data quality: {summary}")
    if rc != 0:
        raise AirflowException(f"dq.py crashed (exit {rc}): {out.splitlines()[-1] if out else ''}")
    if summary.startswith("DQ WARN"):
        lib.alert(f":warning: lakehouse dq_checks warning (gold still published): {summary}")
    return summary


with DAG("dq_checks", schedule="*/5 * * * *", start_date=START, catchup=False, max_active_runs=1,
         default_args=DEFAULTS, dagrun_timeout=dt.timedelta(minutes=4), tags=["quality"],
         doc_md="Soda checks on silver (current state). Record-level failures -> s3://lakehouse/quarantine/dq; "
                "any blocking failure fails the task and alerts. The gold gate runs the same suite on every build.") as dq_dag:
    PythonOperator(task_id="soda_scan_silver", python_callable=run_dq_checks,
                   execution_timeout=dt.timedelta(minutes=3))


# ------------------------------------------------------------------ dbt_run

def run_gold_cycle(**_):
    rc, out = lib.exec_in("dbt", ["bash", "/dbt/cycle.sh", "airflow"])
    line = out.strip().splitlines()[0] if out.strip() else ""
    if rc == 2:
        raise AirflowException(f"gold NOT published, blocked by data quality: {line}")
    if rc != 0:
        raise AirflowException(f"gold NOT published, dbt build failed: {line}")
    return line


with DAG("dbt_run", schedule="*/5 * * * *", start_date=START, catchup=False, max_active_runs=1,
         default_args=DEFAULTS, dagrun_timeout=dt.timedelta(minutes=4), tags=["gold"],
         doc_md="One gold cycle: dbt build (snapshot + models + tests) -> DQ gate -> atomic publish. "
                "Same script as the 30 s loop; flock makes them take turns.") as dbt_dag:
    PythonOperator(task_id="build_dq_gate_publish", python_callable=run_gold_cycle,
                   execution_timeout=dt.timedelta(minutes=3))


# ------------------------------------------------------------------ optimize (nightly)

def run_optimize(run_id, dag_run, data_interval_end, **_):
    # catchup=False still runs the LATEST missed interval as soon as Airflow starts. For a job that
    # pauses streaming that meant an unplanned daytime pause on the first `make airflow-up`
    # (ADR-033). A scheduled run more than 2 h after its slot is skipped; manual triggers always run.
    late = dt.datetime.now(dt.timezone.utc) - data_interval_end
    if dag_run.run_type == "scheduled" and late > dt.timedelta(hours=2):
        raise AirflowSkipException(f"nightly slot {data_interval_end} missed by {late}; next one tonight")
    with lib.streaming_paused(f"optimize {run_id}"):
        return lib.spark_job("/opt/project/spark/optimize.py", "silver", "--zorder")


with DAG("optimize", schedule="0 2 * * *", start_date=START, catchup=False, max_active_runs=1,
         default_args={**DEFAULTS, "on_failure_callback": lib.on_writer_failure, "retries": 1,
                       "retry_delay": dt.timedelta(minutes=5)}, tags=["maintenance"],
         doc_md="Pause streaming, OPTIMIZE ZORDER BY key + VACUUM RETAIN 168 HOURS on every silver table, "
                "resume streaming (always). Gold is a DuckDB file rebuilt every cycle: nothing to compact.") as opt_dag:
    PythonOperator(task_id="optimize_vacuum_silver", python_callable=run_optimize, pool="streaming_writer",
                   execution_timeout=dt.timedelta(minutes=30))


# ------------------------------------------------------------------ backfill_silver (manual)

def run_backfill(params, run_id, **_):
    if params["end_date"] < params["start_date"]:
        raise AirflowException("end_date is before start_date")
    with lib.streaming_paused(f"backfill_silver {run_id}"):
        out = lib.spark_job("/opt/project/streaming/backfill_silver.py", "--table", params["table"],
                            "--start", params["start_date"], "--end", params["end_date"])
    return lib.last_line(out, "BACKFILL ")


with DAG("backfill_silver", schedule=None, start_date=START, catchup=False, max_active_runs=1,
         default_args={**DEFAULTS, "on_failure_callback": lib.on_writer_failure}, tags=["maintenance"],
         render_template_as_native_obj=True,
         params={"table": Param("orders", enum=TABLES),
                 "start_date": Param(str(START.date()), type="string", format="date"),
                 "end_date": Param(str(dt.date.today()), type="string", format="date")},
         doc_md="Rebuild/repair one silver table from bronze for [start_date, end_date] (UTC, inclusive), "
                "same transform as the stream, trigger(availableNow=True). Safe on healthy data thanks to "
                "the LSN guard (ADR-032). Streaming is paused while it writes.") as bf_dag:
    PythonOperator(task_id="backfill_from_bronze", python_callable=run_backfill, pool="streaming_writer",
                   execution_timeout=dt.timedelta(minutes=45))
