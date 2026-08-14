FROM golang:1.26.4 as go-builder

WORKDIR /usr/raft-application/

# NOTE: Cache this
COPY go.mod go.sum ./

RUN go mod tidy

COPY . .


RUN CGO_ENABLED=0 go build -v -o fsm .

# Build Java
FROM maven:3.9-eclipse-temurin-21 AS java-builder

WORKDIR /usr/jkvs-app/

COPY ./jkvs .

RUN pwd 
RUN ls jkvs

# outputs to ./target/jkvs-server.jar
RUN mvn package -DskipTests 


FROM eclipse-temurin:21-jre

WORKDIR /persona-mp3/jkvs-raft/

COPY --from=go-builder /usr/raft-application/fsm /persona-mp3/jkvs-raft/
COPY --from=go-builder /usr/raft-application/cluster-config.toml /persona-mp3/jkvs-raft/
COPY --from=java-builder /usr/jkvs-app/target/jkvs-server.jar /persona-mp3/jkvs-raft/

COPY entrypoint.sh /persona-mp3/jkvs-raft/entrypoint.sh
RUN chmod +x /persona-mp3/jkvs-raft/entrypoint.sh
RUN ls 
RUN pwd

# ports are described in the cluster-config.toml
EXPOSE 5001 5002 6061 9090
ENTRYPOINT ["./entrypoint.sh"]
