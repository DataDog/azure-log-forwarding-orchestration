FROM registry.ddbuild.io/images/mirror/golang:1.26.2 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates

WORKDIR /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder
# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the code into the container
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" cmd/forwarder/forwarder.go

FROM scratch
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder/forwarder /forwarder

# Set the version
ARG VERSION_TAG="latest"
ENV VERSION_TAG=$VERSION_TAG

CMD ["/forwarder"]
