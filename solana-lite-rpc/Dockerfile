FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev

WORKDIR /usr/workspace/

RUN git clone "https://github.com/dexterlaboss/solana-lite-rpc.git"

WORKDIR /usr/workspace/solana-lite-rpc

RUN cargo build

RUN chmod +x /usr/workspace/solana-lite-rpc/target/debug/solana-lite-rpc

FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y bzip2 vim curl

COPY --from=builder /usr/workspace/solana-lite-rpc/target/debug/solana-lite-rpc /usr/local/bin/solana-lite-rpc

ENTRYPOINT ["/usr/local/bin/solana-lite-rpc", "--rpc-hbase-address=hbase:9090", "--log-path=/usr/local/bin/solana-lite-rpc-log", "--log"]
