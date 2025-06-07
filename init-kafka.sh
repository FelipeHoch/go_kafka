#!/bin/bash
echo "Criando usuário SCRAM-SHA-256 no Kafka..."

kafka-configs --zookeeper zookeeper:2181 \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=4096,password=kafka-secret]' \
  --entity-type users \
  --entity-name kafka

echo "Inicializando Kafka..."

/etc/confluent/docker/run