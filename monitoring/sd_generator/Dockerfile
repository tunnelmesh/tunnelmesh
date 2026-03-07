FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/tunnelmesh-prometheus-sd-generator/ ./cmd/tunnelmesh-prometheus-sd-generator/
COPY internal/promsd/ ./internal/promsd/

RUN go build -o sd-generator ./cmd/tunnelmesh-prometheus-sd-generator

FROM alpine:3.23

RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/sd-generator .

ENTRYPOINT ["/app/sd-generator"]
