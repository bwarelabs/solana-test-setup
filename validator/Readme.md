# Validator

This folder contains a Docker-based setup for the Validator service. The service is a custom Solana Warehouse test node that generates a local network and simulates transactions while uploading them to the BigTable emulator.

## Environment Variables
This container uses the following environment variables:
1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.
2. **GOOGLE_APPLICATION_CREDENTIALS**: A placeholder for Google credentials. Not required for the emulator but set for compatibility.