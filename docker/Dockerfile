# Multi-stage build: compile the CD-Raft node + client, then ship them in a
# slim image that also carries iproute2 so each container can self-inject real
# kernel-level (tc/netem) per-link latency at startup.
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cdraft        ./cmd/cdraft \
 && CGO_ENABLED=0 go build -o /out/cdraft-client ./cmd/cdraft-client \
 && CGO_ENABLED=0 go build -o /out/cdraft-mover  ./cmd/cdraft-mover

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 iputils-ping ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/cdraft        /usr/local/bin/cdraft
COPY --from=build /out/cdraft-client /usr/local/bin/cdraft-client
COPY --from=build /out/cdraft-mover  /usr/local/bin/cdraft-mover
COPY docker/entrypoint.sh            /usr/local/bin/entrypoint.sh
COPY docker/cluster.docker.json      /etc/cd-raft/cluster.json
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
