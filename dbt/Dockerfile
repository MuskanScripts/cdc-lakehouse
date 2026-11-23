# One image for the gold layer: dbt-duckdb (build loop) and the Streamlit dashboard. Both use the
# SAME duckdb version, which matters: a DuckDB file written by one version is read by the other.
FROM python:3.12-slim
RUN pip install --no-cache-dir \
      dbt-core==1.9.1 dbt-duckdb==1.9.1 duckdb==1.1.3 deltalake==0.22.3 pyarrow \
      streamlit==1.40.1 pandas boto3 confluent-kafka==2.6.1
WORKDIR /dbt
