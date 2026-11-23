# One image for the gold layer: dbt-duckdb (build loop), the data quality gate (Soda), the Streamlit
# dashboard and the Prometheus exporter. All of them use the SAME duckdb version, which matters: a
# DuckDB file written by one version is read by the others.
FROM python:3.12-slim
RUN pip install --no-cache-dir \
      dbt-core==1.9.1 dbt-duckdb==1.9.1 duckdb==1.1.3 deltalake==0.22.3 pyarrow \
      streamlit==1.40.1 pandas boto3 confluent-kafka==2.6.1 "psycopg[binary]==3.2.3" pytest==8.3.4
# Soda Core in its own venv (ADR-030): it pins protobuf<5 (dbt needs >=5) and duckdb<1.1 (gold is
# written by 1.1.3). duckdb is overridden to 1.1.3 with --no-deps: Soda only uses the basic
# connect/execute API. setuptools provides the `distutils` module Soda still imports on Python 3.12.
RUN python -m venv /opt/soda \
 && /opt/soda/bin/pip install --no-cache-dir soda-core-duckdb==3.5.6 setuptools==75.6.0 \
      deltalake==0.22.3 pyarrow "psycopg[binary]==3.2.3" pyyaml pytest==8.3.4 \
 && /opt/soda/bin/pip install --no-cache-dir --no-deps duckdb==1.1.3 \
 && mkdir -p /root/.soda && printf 'send_anonymous_usage_stats: false\n' > /root/.soda/config.yml
WORKDIR /dbt
