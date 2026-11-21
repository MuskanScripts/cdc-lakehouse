ARG CP_VERSION=7.7.1
FROM confluentinc/cp-kafka-connect:${CP_VERSION}

# Debezium's own images no longer ship the Confluent Avro converter, so we start from
# Confluent's Connect image (Avro + Schema Registry built in) and add the Debezium
# Postgres connector plugin from Maven Central.
ARG DEBEZIUM_VERSION=2.7.3.Final
USER root
RUN mkdir -p /usr/share/confluent-hub-components \
 && curl -fsSL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    | tar -xz -C /usr/share/confluent-hub-components \
 && chown -R appuser:appuser /usr/share/confluent-hub-components
USER appuser
