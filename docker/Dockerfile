# Stage 1: Build
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app

# Copy go mod files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false \
    -ldflags "-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME}" \
    -o /tunnelmesh ./cmd/tunnelmesh

# Stage 2: Runtime
FROM alpine:3.23

# Install runtime dependencies and create required directories
RUN apk add --no-cache \
    iproute2 \
    iputils \
    bash \
    curl \
    jq \
    gettext \
    ca-certificates && \
    mkdir -p /root/.tunnelmesh /etc/tunnelmesh

# Copy binary from builder
COPY --from=builder /tunnelmesh /usr/local/bin/tunnelmesh
RUN chmod +x /usr/local/bin/tunnelmesh

ENTRYPOINT ["/usr/local/bin/tunnelmesh"]
