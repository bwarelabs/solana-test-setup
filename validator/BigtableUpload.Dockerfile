FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev

WORKDIR /usr/workspace/

RUN git clone "https://github.com/agave/agave.git"

WORKDIR /usr/workspace/agave/validator
RUN cargo build --release

RUN chmod +x /usr/workspace/agave/target/release/solana-test-validator

FROM ubuntu:latest

RUN mkdir -p /usr/workspace/agave/test-ledger && \
    chmod -R 755 /usr/workspace/agave/test-ledger

RUN apt-get update && \
    apt-get install -y bzip2 vim

COPY --from=builder /usr/workspace/agave/target/release/solana-test-validator /usr/local/bin/solana-test-validator

ENTRYPOINT ["/usr/local/bin/solana-test-validator", "--enable-bigtable-ledger-upload"]
