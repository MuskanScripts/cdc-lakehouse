"""Helpers for the lakehouse DAGs (no DAG in this file).

Airflow only ORCHESTRATES here (ADR-029). Every task runs its work inside a container of the stack
through the Docker socket: DQ and gold cycles in `dbt` (which has dbt, Soda, delta-rs), Spark jobs
in `spark`, and pausing means stopping `streaming`. The Airflow image needs nothing extra (the
docker SDK ships with it) and no second copy of dbt, Spark or Soda.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from contextlib import contextmanager

import docker
from airflow.exceptions import AirflowException

log = logging.getLogger("lakehouse")
ALERT_LOG = os.environ.get("ALERT_LOG", "/opt/airflow/logs/alerts.log")
PAUSE_VAR = "streaming_paused_by_airflow"   # survives a killed task: the retry knows to resume (ADR-033)
# exclusive.sh: one writer job at a time, an orphan from a killed attempt is stopped first (ADR-033).
# local[2]: leave cores for Airflow, dbt and Postgres; with all cores busy the webserver stalled.
SPARK_SUBMIT = ["bash", "/opt/project/spark/exclusive.sh", "--master", "local[2]", "--driver-memory", "1500m",
                "--packages", "org.apache.spark:spark-avro_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1,"
                              "org.apache.hadoop:hadoop-aws:3.3.4",
                "--conf", "spark.jars.ivy=/root/.ivy2"]


def _api():
    return docker.APIClient(base_url="unix://var/run/docker.sock", timeout=60)


def exec_in(container: str, cmd: list, env: dict | None = None) -> tuple[int, str]:
    """Run `cmd` inside a running container, stream its output into the task log, return (rc, output)."""
    api = _api()
    ex = api.exec_create(container, cmd, environment=env or {})
    out, buf = [], ""
    for chunk in api.exec_start(ex["Id"], stream=True):
        buf += chunk.decode(errors="replace")
        *lines, buf = buf.split("\n")
        for line in lines:
            log.info("[%s] %s", container, line)
            out.append(line)
    if buf:
        log.info("[%s] %s", container, buf)
        out.append(buf)
    rc = api.exec_inspect(ex["Id"])["ExitCode"]
    return rc, "\n".join(out)


def last_line(output: str, prefix: str) -> str:
    lines = [ln for ln in output.splitlines() if ln.startswith(prefix)]
    return lines[-1] if lines else ""


def ensure_running(name: str) -> None:
    c = docker.from_env().containers.get(name)
    if c.status != "running":
        log.info("starting container %s (was %s)", name, c.status)
        c.start()


def alert(text: str) -> None:
    """Slack if SLACK_WEBHOOK_URL is set, otherwise log only. Always appended to logs/alerts.log."""
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        with open(ALERT_LOG, "a") as f:
            f.write(f"{stamp} {text}\n")
    except OSError:
        pass
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        log.warning("ALERT (SLACK_WEBHOOK_URL not set, logged only): %s", text)
        return
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            log.warning("ALERT sent to Slack (HTTP %s): %s", r.status, text)
    except Exception as e:  # noqa: BLE001  an alerting outage must not hide the original failure
        log.error("ALERT could not be sent to Slack (%s): %s", e, text)


def on_failure(context) -> None:
    ti = context["task_instance"]
    alert(f":red_circle: lakehouse {ti.dag_id}.{ti.task_id} FAILED (run {context.get('run_id')}): "
          f"{context.get('exception')}")


def resume_streaming() -> None:
    """Start streaming again if an Airflow task paused it (marker in an Airflow Variable)."""
    from airflow.models import Variable
    if Variable.get(PAUSE_VAR, default_var=None) is None:
        return
    c = docker.from_env().containers.get("streaming")
    log.info("resuming streaming (paused by %s)", Variable.get(PAUSE_VAR, default_var="?"))
    c.start()
    for _ in range(30):
        c.reload()
        if c.status == "running":
            break
        time.sleep(1)
    if c.status != "running":
        raise AirflowException(f"streaming did not come back (status {c.status})")
    Variable.delete(PAUSE_VAR)


@contextmanager
def streaming_paused(owner: str, timeout_s: int = 60):
    """Stop the streaming driver for the duration of the block, ALWAYS start it again.

    A stop mid-batch is safe: the checkpoints and idempotent writes were proven with 10 SIGKILLs
    (ADR-021); a stop is gentler (SIGTERM first). `docker stop` also tells `restart: unless-stopped`
    not to restart it until we start it.

    The pause is recorded in an Airflow Variable BEFORE the stop. If this task process is killed
    (no `finally`), the retry sees the marker and resumes at its end, and the failure callback
    resumes too (ADR-033). Without the marker a retry found streaming stopped, concluded "it was
    not running" and left it down.
    """
    from airflow.models import Variable
    c = docker.from_env().containers.get("streaming")
    if c.status == "running":
        Variable.set(PAUSE_VAR, owner)
        log.info("pausing streaming (docker stop, timeout %ss)", timeout_s)
        c.stop(timeout=timeout_s)
    elif Variable.get(PAUSE_VAR, default_var=None) is not None:
        log.info("streaming already paused by %s (an earlier attempt); resuming it at the end",
                 Variable.get(PAUSE_VAR))
    else:
        log.info("streaming is not running and was not paused by Airflow: leaving it stopped")
    try:
        yield
    finally:
        resume_streaming()


def on_writer_failure(context) -> None:
    """Failure callback for tasks that pause streaming: stop a leftover job, resume, alert."""
    try:
        exec_in("spark", ["bash", "/opt/project/spark/exclusive.sh", "--kill"])
        resume_streaming()
    except Exception as e:  # noqa: BLE001
        log.error("could not clean up after the failed writer task: %s", e)
    on_failure(context)


def spark_job(script: str, *args: str) -> str:
    ensure_running("spark")
    try:
        rc, out = exec_in("spark", SPARK_SUBMIT + [script, *args])
    except BaseException:
        # timeout / SIGTERM of this task: the exec'd job would keep running and keep writing after
        # streaming resumes. Stop it before the `finally` of streaming_paused() runs.
        exec_in("spark", ["bash", "/opt/project/spark/exclusive.sh", "--kill"])
        raise
    if rc != 0:
        raise AirflowException(f"{script} exited {rc}")
    return out
