FROM registry.ddbuild.io/images/mirror/golang:1.24.0 AS builder
WORKDIR /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder
# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the code into the container
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" cmd/forwarder/forwarder.go

FROM registry.ddbuild.io/images/mirror/alpine:latest
WORKDIR /
COPY --from=builder /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder/forwarder /forwarder

# Set the version
ARG VERSION_TAG="latest"
ENV VERSION_TAG=$VERSION_TAG

CMD ["/forwarder"]
