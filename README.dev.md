# Kafka Consumer Microservice

Este é um microserviço que atua como consumidor de mensagens do Kafka, processando payloads e encaminhando-os para outro microserviço via REST API.

## Como executar

Para executar o microserviço, apenas rode os seguintes comandos (Analise o .env, caso deseje utilizar o target service):

```bash
git clone https://github.com/felipehoch/go_kafka.git

cd go_kafka

docker compose up 
```

Os serviços necessários para o funcionamento do microserviço serão iniciados. O Kafka possuirá um delay de 10 segundos para que o Zookeeper possa ser iniciado.

## Como testar

Para testar o microserviço, você pode acessar a URL `http://localhost:7777` que dará acesso ao Kafka UI, lá você poderá produzir mensagens nos tópicos.

## Disclaimer

Implementei um tracing de forma simples apenas com UUIDs, dado o escopo do teste, contudo em um ambiente real, deveria ser utilizado um tracing mais robusto, como o [OpenTelemetry](https://opentelemetry.io/).