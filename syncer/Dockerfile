# Step 1: Build the Rust project
FROM rust:1.80.1-bookworm as rust-builder

WORKDIR /usr/workspace

RUN git clone "https://github.com/bwarelabs/solana-syncer.git" -b "cleanup"

WORKDIR /usr/workspace/solana-syncer/solana-bigtable

# this is a small hack to allow building the dependencies first
# so incremental builds are faster
RUN echo "// dummy file" > src/lib.rs \
    && cargo build --release

RUN sh -c "echo '// updated' >> src/lib.rs && cargo build --release"

# Step 2: Build the Java project
FROM eclipse-temurin:22 as java-builder

RUN apt-get update && \
    apt-get install -y maven vim

COPY --from=rust-builder /usr/workspace/solana-syncer /usr/workspace/solana-syncer
WORKDIR /usr/workspace/solana-syncer

RUN mvn dependency:resolve

RUN mvn package

# Step 3: Build the final image
FROM eclipse-temurin:22

RUN apt-get update && \
    apt-get install -y vim

RUN mkdir /app
WORKDIR /app

COPY --from=rust-builder /usr/workspace/solana-syncer/solana-bigtable/target/release/libsolana_bigtable.so /usr/lib/libsolana_bigtable.so

COPY --from=java-builder /usr/workspace/solana-syncer/target/syncer-1.0-SNAPSHOT.jar /app/target/syncer-1.0-SNAPSHOT.jar
COPY --from=java-builder /usr/workspace/solana-syncer/start.sh /app/start.sh

ENTRYPOINT ["/app/start.sh"]
