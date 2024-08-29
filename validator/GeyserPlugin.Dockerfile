FROM rust:bookworm as builder

ENV PATH=/usr/local/cargo/bin:/home/admin/.local/bin:/home/admin/.local/share/solana/install/active_release/bin:$PATH
ENV RUST_LOG=debug
ENV RUST_BACKTRACE=1

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release protobuf-compiler clang libclang-dev

WORKDIR /home/admin

RUN git clone --depth 1 --single-branch --branch v1.18.22 "https://github.com/anza-xyz/agave.git"

WORKDIR /home/admin/agave/validator
RUN cargo build --release

WORKDIR /home/admin
RUN git clone https://github.com/bwarelabs/solana-cos-plugin.git

WORKDIR /home/admin/solana-cos-plugin
RUN cargo build --release

FROM ubuntu:latest

COPY --from=builder /home/admin/agave/target/release/solana-test-validator /usr/local/bin/solana-test-validator
COPY --from=builder /home/admin/solana-cos-plugin/target/release/libsolana_cos_plugin.so /usr/local/lib/libsolana_cos_plugin.so

COPY config.json /usr/local/bin/config.json

RUN apt-get update && \
    apt-get install -y bzip2 vim

ENTRYPOINT ["/usr/local/bin/solana-test-validator", "--geyser-plugin-config", "/usr/local/bin/config.json"]
