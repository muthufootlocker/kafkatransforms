FROM confluentinc/cp-kafka-connect:6.1.0
USER root
ENV CONNECT_PLUGIN_PATH /usr/share/confluent-hub-components/
ENV CONNECT_BOOTSTRAP_SERVERS broker:29092
ENV CONNECT_GROUP_ID compose-connect-group
ENV CONNECT_CONFIG_STORAGE_TOPIC docker-connect-configs
ENV CONNECT_OFFSET_STORAGE_TOPIC docker-connect-offsets
ENV CONNECT_STATUS_STORAGE_TOPIC docker-connect-status
ENV CONNECT_INTERNAL_KEY_CONVERTER org.apache.kafka.connect.storage.StringConverter
ENV CONNECT_INTERNAL_VALUE_CONVERTER org.apache.kafka.connect.json.JsonConverter
ENV CONNECT_KEY_CONVERTER org.apache.kafka.connect.storage.StringConverter
ENV CONNECT_VALUE_CONVERTER org.apache.kafka.connect.json.JsonConverter
ENV CONNECT_REST_ADVERTISED_HOST_NAME localhost
ENV CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR "1"
ENV CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR "1"
ENV CONNECT_STATUS_STORAGE_REPLICATION_FACTOR "1"
COPY ./key-map /usr/share/confluent-hub-components/
RUN curl -O http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz
RUN mkdir confluent-hub-client-latest
RUN tar -xf confluent-hub-client-latest.tar.gz -C confluent-hub-client-latest

RUN ./confluent-hub-client-latest/bin/confluent-hub install --no-prompt --verbose --component-dir /usr/share/confluent-hub-components debezium/debezium-connector-postgresql:1.2.1