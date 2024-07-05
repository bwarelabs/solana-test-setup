# Syncer

This folder contains a Docker-based setup for the Syncer service. The syncer reads data from the BigTable emulator and writes it to sequencefiles locally.

## How to run the Syncer

* exec into the container
`docker compose exec syncer /bin/bash`

* start the process
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar`

* it writes each data into `./output/sequencefiles/{table_name}` directory

## Environment Variables
This container uses the following environment variables:
1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.

PS: there is a new version in progress on branch SOL-18 which watches the data written by the geyser plugin and uploads it directly in COS, and a new class that writes data from bigtable and writes to COS for the initial sync, but we still test the code, it will reach main soon.
