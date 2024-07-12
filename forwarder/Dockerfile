# Build
FROM golang:1.22.2 AS buildStage

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go build -o /app/forwarder ./cmd/forwarder

# Deploy
FROM alpine:latest

WORKDIR /

COPY --from=buildStage /app/forwarder /forwarder

USER nonroot:nonroot

ENTRYPOINT ["/forwarder"]