ARG GOLANG_VERSION=1.25-trixie
ARG DEBIAN_VERSION=trixie

FROM golang:${GOLANG_VERSION} AS builder

RUN go install github.com/minio/minio@v0.0.0-20250422221226-0d7408fc9969



FROM debian:${DEBIAN_VERSION}

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y gzip xz-utils tar

COPY --chmod=777 --from=builder /go/bin/minio /usr/local/bin/minio

COPY minio-chargy-data.tar.gz /tmp/minio-preload-data.tar.gz

COPY --chmod=777 entrypoint.sh /usr/local/bin/entrypoint.sh

VOLUME /data

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

CMD ["server", "/data", "--address", ":9000", "--console-address", ":9001"]
