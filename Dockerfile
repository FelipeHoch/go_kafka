FROM golang:1.24.4-alpine AS builder

RUN apk add --no-cache \
    git \
    ca-certificates \
    tzdata \
    gcc \
    g++ \
    pkgconfig \
    librdkafka-dev \
    musl-dev

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -a -tags musl -o main .

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata librdkafka

RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /app/main .

RUN chown -R appuser:appgroup /app

USER appuser

CMD ["./main"] 