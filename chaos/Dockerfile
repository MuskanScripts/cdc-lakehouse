FROM python:3.12-slim
RUN pip install --no-cache-dir "psycopg[binary]==3.2.3" deltalake==0.22.3 pyarrow
WORKDIR /app
COPY . /app
ENTRYPOINT ["python"]
