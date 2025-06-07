# Kafka Consumer Microservice

Este é um microserviço que atua como consumidor de mensagens do Kafka, processando payloads e encaminhando-os para outro microserviço via REST API.

## Como executar

Para executar o microserviço, apenas rode os seguintes comandos (Analise o .env, caso deseje utilizar o target service):

```bash
git clone git@github.com:FelipeHoch/go_kafka.git

cd go_kafka

docker compose up 

```

Os serviços necessários para o funcionamento do microserviço serão iniciados.

## Como testar

Para testar o microserviço, você pode acessar a URL `http://localhost:7777` que dará acesso ao Kafka UI, lá você poderá produzir mensagens nos tópicos.

Para testar a ordem, você pode produzir uma mensagem com o seguinte payload:

```json
{
    "ordemDeVenda": "1234567890",
    "etapaAtual": "FATURADO"
}
```

## Disclaimer

Implementei um tracing de forma simples apenas com UUIDs, dado o escopo do teste, contudo em um ambiente real, deveria ser utilizado um tracing mais robusto, como o [OpenTelemetry](https://opentelemetry.io/).

Para validar a ordem, tomei a liberdade de validar o status da ordem, caso o status seja diferente de `FATURADO`, a mesma será movida para o tópico de DLQ.