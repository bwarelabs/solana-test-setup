FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev

WORKDIR /usr/workspace/

RUN git clone "https://github.com/bwarelabs/solana.git" -b "SOL-12-Implement-a-test-setup-for-the-project"

WORKDIR /usr/workspace/solana/validator

RUN cargo build

RUN chmod +x /usr/workspace/solana/target/debug/solana-validator

FROM ubuntu:latest

RUN mkdir -p /usr/workspace/solana/test-ledger && \
    chmod -R 755 /usr/workspace/solana/test-ledger

RUN apt-get update && \
    apt-get install -y bzip2 vim

COPY --from=builder /usr/workspace/solana/target/debug/solana-test-validator /usr/local/bin/solana-test-validator

ENTRYPOINT ["/usr/local/bin/solana-test-validator", "--enable-bigtable-ledger-upload"]
