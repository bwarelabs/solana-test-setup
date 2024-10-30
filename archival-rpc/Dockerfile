FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev

WORKDIR /usr/workspace/

RUN git clone "https://github.com/dexterlaboss/archival-rpc.git"

WORKDIR /usr/workspace/archival-rpc

RUN cargo build --release

RUN chmod +x /usr/workspace/archival-rpc/target/release/archival-rpc

FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y bzip2 vim curl

COPY --from=builder /usr/workspace/archival-rpc/target/release/archival-rpc /usr/local/bin/archival-rpc

ENTRYPOINT ["/usr/local/bin/archival-rpc", "--rpc-hbase-address=hbase:9090", "--log-path=/usr/local/bin/archival-rpc-log", "--log"]
