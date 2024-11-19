FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev libudev-dev

WORKDIR /usr/workspace/

RUN git clone "https://github.com/bwarelabs/solana-bigtable-hbase-adapter.git"

WORKDIR /usr/workspace/solana-bigtable-hbase-adapter

RUN cargo build

RUN chmod +x /usr/workspace/solana-bigtable-hbase-adapter/target/debug/server

FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y bzip2 vim curl

COPY --from=builder /usr/workspace/solana-bigtable-hbase-adapter/target/debug/server /usr/local/bin/solana-bigtable-hbase-adapter

ENTRYPOINT ["/usr/local/bin/solana-bigtable-hbase-adapter"]
