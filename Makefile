.PHONY: help up down clean ps logs register status topics consume simulate simulate-fast check spark-smoke psql slot-lag test stream-up stream-logs stream-down reconcile latency drift late-demo late-stats bronze-check chaos

TABLE ?= orders
RATE  ?= 200
DURATION ?= 0

help:            ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n",$$1,$$2}'

up:              ## start the stack and register the Debezium connector
	docker compose up -d --build
	./scripts/register_connector.sh

down:            ## stop containers (keeps data)
	docker compose --profile sim down

clean:           ## stop and delete all data volumes
	docker compose --profile sim down -v

ps:              ## container status
	docker compose ps

logs:            ## follow Connect logs (most problems show up here)
	docker compose logs -f connect

register:        ## (re)register the connector
	./scripts/register_connector.sh

status:          ## connector + task status
	@curl -s http://localhost:8083/connectors/ecom-postgres/status; echo

topics:          ## list Kafka topics
	docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

consume:         ## tail one topic as JSON: make consume TABLE=payments
	docker compose exec schema-registry kafka-avro-console-consumer \
	  --bootstrap-server kafka:29092 --topic ecom.public.$(TABLE) --from-beginning \
	  --property schema.registry.url=http://localhost:8081 --property print.key=true

simulate:        ## run simulator: make simulate RATE=500 DURATION=120
	docker compose run --rm --build simulator --rate $(RATE) --duration $(DURATION)

simulate-fast:   ## 5000 changes/sec load test for 60s
	docker compose run --rm --build simulator --rate 5000 --duration 60 --fast

check:           ## Week-1 acceptance: c/u/d events on every topic
	./scripts/check_events.sh

spark-smoke:     ## Spark writes/reads a Delta table on MinIO
	docker compose exec spark /opt/spark/bin/spark-submit \
	  --packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4 \
	  --conf spark.jars.ivy=/root/.ivy2 \
	  /opt/project/spark/smoke_delta.py

psql:            ## open psql on the source DB
	docker compose exec postgres psql -U ecom -d ecom

test:            ## all unit tests, inside the spark container (has Java + PySpark)
	docker compose up -d spark
	docker compose exec -T -e PYTHONPATH=/opt/project:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip spark \
	  bash -c "pip install -q pytest faker 'psycopg[binary]' 2>&1 | tail -1; cd /opt/project && python3 -m pytest -q -p no:cacheprovider tests"

stream-up:       ## start bronze + silver streaming (one Spark driver, restarts on failure)
	docker compose --profile stream up -d streaming

stream-logs:     ## follow the streaming driver logs
	docker compose logs -f --tail 50 streaming

stream-down:     ## stop streaming (checkpoints keep its place)
	docker compose --profile stream stop streaming

reconcile:       ## compare Postgres vs silver; non-zero exit on any difference (WAIT=300 retries while silver catches up)
	docker compose run --rm --build reconcile reconcile.py --wait $(or $(WAIT),0)

latency:         ## end-to-end latency p50/p95/p99 (MINUTES=5 window)
	docker compose run --rm reconcile latency.py --minutes $(or $(MINUTES),5)

drift:           ## schema drift drill: ADD COLUMN while running, time it to silver (COL=coupon_code TBL=orders)
	docker compose run --rm --build reconcile drift.py --column $(or $(COL),coupon_code) --table $(or $(TBL),orders)

late-demo:       ## send a 30-minute-late order and prove it lands in late_events, not in orders_per_minute
	docker compose run --rm reconcile late_demo.py

late-stats:      ## order event delay percentiles (basis for the watermark); MINUTES=60 limits the window
	docker compose run --rm reconcile event_delay.py --minutes $(or $(MINUTES),0)

bronze-check:    ## every Kafka offset exactly once in bronze?
	docker compose run --rm reconcile bronze_check.py

chaos:           ## simulator + 10 random kills of streaming/connect/kafka, then reconcile (RATE=100 KILLS=10)
	RATE=$(or $(RATE),100) KILLS=$(or $(KILLS),10) ./chaos/run_chaos.sh

slot-lag:        ## WAL retained by the Debezium slot (alert on this in prod)
	docker compose exec postgres psql -U ecom -d ecom -c \
	  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;"
