# Google BigTable Emulator

This folder contains a Docker-based setup for BigTable Emulator. The emulator is used to simulate Google BigTable for local testing without needing to interact with Google Cloud.

The **validator** service is dependent on the BigTable emulator since it writes the data to it.
The tables are created automaticaly when the **validator** service starts. 

The table are:
- `blocks`
- `entries`
- `tx`
- `tx-by-addr`

The emulator is configured to use the project `emulator` and the instance `solana-ledger`.

Also, the emulator is exposed on port `8086`.

## Common used commands for development
- List tables: `cbt -project emulator -instance solana-ledger ls`
- Read table: `cbt -project emulator -instance solana-ledger read <table>`

## Environment Variables
This container uses the following environment variables:
1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.
2. **GOOGLE_APPLICATION_CREDENTIALS**: A placeholder for Google credentials. Not required for the emulator but set for compatibility.