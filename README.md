# Kafka Consumer Microservice

Este é um microserviço que atua como consumidor de mensagens do Kafka, processando payloads e encaminhando-os para outro microserviço via REST API.

## Descrição

O microserviço é responsável por:
1. Consumir mensagens de um tópico específico do Kafka
2. Processar o payload recebido (formato definido em `payload.json`)
3. Encaminhar o payload para outro microserviço via REST API (método PATCH)

## Pré-requisitos

- Go 1.24.1 ou superior
- Apache Kafka v3.6
- Docker (opcional, para ambiente de desenvolvimento)

## Estrutura do Projeto

```
.
├── README.md
├── go.mod
├── go.sum
├── main.go
├── internal/
│   ├── consumer/
│   │   └── kafka.go
│   ├── service/
│   │   └── processor.go
│   └── config/
│       └── config.go
└── .env
```

## Configuração

1. Configure as variáveis de ambiente
Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_USERNAME=exemplo
KAFKA_PASSWORD=exemplo
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT"
KAFKA_GROUP_ID=seu-grupo
KAFKA_TOPIC=seu-topico
KAFKA_TOPIC_DLQ=seu-topico-dlq
TARGET_SERVICE_URL=http://localhost:8080/api/v1
```

## Como Executar

Avaliaremos o código com nossos sistemas

1. Crie um ambiente em docker com Kafka v3.6 para realizar seu desenvolvimento e testes.

## Dependências Principais

- github.com/confluentinc/confluent-kafka-go/v2
- github.com/spf13/viper (para gerenciamento de variáveis de ambiente)

## Formato do Payload

O payload esperado segue o formato definido em `payload.json`. O mesmo payload será encaminhado para o serviço destino via método PATCH.

## Logs

O serviço registra logs para:
- Conexão com o Kafka
- Recebimento de mensagens
- Processamento do payload
- Chamadas ao serviço destino
- Erros durante o processamento

## Contribuição

1. Faça um repositório no seu GitHub para ser compartilhado com o time.

