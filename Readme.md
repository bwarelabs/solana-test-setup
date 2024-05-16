# Solana Test Environment with Google BigTable Emulator

This repository contains a Docker-based setup for a Solana test environment integrated with a Google BigTable emulator. The environment includes 4 main services:

1. **BigTable Emulator**: Simulates Google BigTable for local testing without needing to interact with Google Cloud.
2. **Solana Test Validator**: A custom Solana Warehouse test node that generates a local network and simulates transactions while uploading them to the BigTable emulator.
3. **Syncer**: A custom service that reads from the BigTable emulator and writes data to sequencefiles locally.
4. **HBASE**: A Hadoop database that can be used to import the sequencefiles generated by the Syncer.

The setup is ideal for development and testing of migration Solana from Google BigTable to HBase.

## Requirements

- Docker
- Docker Compose

## Getting Started

### Clone the Repository

First, clone this repository to your local machine:

```bash
git clone https://github.com/bwarelabs/solana-bigtable-integration.git
cd solana-bigtable-integration
```

# Build and Run the Containers

Run the following command to build and start the containers using Docker Compose:

```bash
docker compose build
docker compose up
```

This command builds the services defined in docker-compose.yml and starts the containers. Here’s what each service does:

1. **bigtable-emulator**: Runs the Google BigTable emulator and exposes it on port 8086.
2. **validator**: Builds from a Dockerfile located in the validator/ directory. It depends on the BigTable emulator and starts once the emulator is up and running. It also sets up environment variables required for the Solana node to interact with the BigTable emulator.
3. **syncer**: Builds from a Dockerfile located in the syncer/ directory. It depends on the BigTable emulator and starts once the emulator is up and running. The syncer reads data from the BigTable emulator and writes it to sequencefiles.
4. **hbase**: Runs an HBase database and exposes it on port 2181.

# Environment Variables

The containers use the following environment variables:

1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.
2. **GOOGLE_APPLICATION_CREDENTIALS**: A placeholder for Google credentials. Not required for the emulator but set for compatibility.
