FROM golang:1.22.2

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go build -o /app/forwarder ./cmd/forwarder.go


ENTRYPOINT ["/app/forwarder"]
