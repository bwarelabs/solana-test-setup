FROM rust:latest as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget curl gnupg software-properties-common lsb-release clang libclang-dev maven

WORKDIR /usr/workspace/
RUN git clone "https://github.com/bwarelabs/solana-syncer.git"

WORKDIR /usr/workspace/solana-syncer

RUN mvn dependency:resolve

RUN mvn package
