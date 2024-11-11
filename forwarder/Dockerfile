FROM 486234852809.dkr.ecr.us-east-1.amazonaws.com/golang:1.23.2 AS builder
WORKDIR /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder
# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the code into the container
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build cmd/forwarder/forwarder.go

FROM 486234852809.dkr.ecr.us-east-1.amazonaws.com/alpine:latest
WORKDIR /
COPY --from=builder /usr/local/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder/forwarder /forwarder
CMD ["/forwarder"]
