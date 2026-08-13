# syntax=docker/dockerfile:1
FROM golang:1.26.4

WORKDIR /usr/raft-application/

COPY go.mod go.sum ./

RUN go mod tidy

COPY . .

RUN go build -v -o fsm /usr/raft-application

EXPOSE 5001 5002

CMD ["./fsm" "--config", "config-cluster.toml"]

