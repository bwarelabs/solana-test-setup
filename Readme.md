# Solana Test Environment with Google BigTable Emulator

## Project Overview

The Solana Test Setup is a comprehensive Docker-based environment designed for developers and engineers working with the Solana blockchain. This project enables the simulation, testing, and migration of Solana's data from Google BigTable to alternative storage solutions, such as Tencent Cloud Storage and HBase, without relying on Google Cloud infrastructure.

The environment includes a range of services, from a BigTable emulator to a Solana test validator, Syncer, and HBase, all orchestrated through Docker Compose. This setup is ideal for developing and testing the migration of Solana's massive dataset, ensuring that the blockchain can operate seamlessly across different storage platforms. Whether you're working on syncing historical data, handling real-time transactions, or querying blockchain data, the Solana Test Setup provides a flexible and powerful toolset to support your needs.

## Architecture
![Solana Syncer Architecture](https://github.com/bwarelabs/solana-test-setup/blob/cleanup/solana-syncing-architecture.png?raw=true)

## Services Overview

1. **BigTable Emulator**:
    - Simulates Google BigTable for local testing without needing to interact with Google Cloud.
    - **Port**: `8086`
    - **Start Command**: `docker compose up bigtable-emulator --build`

2. **Solana Test Validator**:
    - A custom Solana Warehouse test node that generates a local network and simulates transactions while uploading them to either the BigTable emulator or the Solana Bigtable HBase Adapter.
    - **Start Command**: `docker compose up validator --build`
    - **Environment Variable**: Set `BIGTABLE_EMULATOR_HOST` to either `bigtable-emulator:8086` or `solana-bigtable-hbase-adapter:50051` depending on your setup.

3. **Syncer**:
    - A service responsible for migrating data from BigTable or local files to Tencent Cloud Storage in sequencefiles format.
    - **Start Command**:
        - For BigTable: `docker compose up syncer-bigtable --build`
        - For Local Files: `docker compose up syncer-local-files --build`
    - **Configuration**: Update `config.properties` with the necessary Tencent Cloud Storage credentials and other settings.

4. **HBase**:
    - A Hadoop database that can be used to import the sequencefiles generated by the Syncer.
    - **Ports**: `16010`, `16020`, `16030`, `2181`, `9090`
    - **Start Command**: `docker compose up hbase --build`

5. **Solana Lite RPC**:
    - A Solana RPC server that can be used to query the Solana network from both BigTable and HBase.
    - **Ports**: `8899`, `8900`
    - **Start Command**: `docker compose up solana-lite-rpc --build`

6. **Bigtable to HBase Adapter**:
    - Mimics the Google BigTable interface, allowing Solana nodes to write data directly to HBase instead of BigTable.
    - **Port**: `50051`
    - **Start Command**: `docker compose up solana-bigtable-hbase-adapter --build`
    - **Environment Variable**: Set `HBASE_HOST` to `hbase:9090`.

## Requirements
- Docker Compose V2

## Usage

### There are multiple setups in this repository:

1. **Solana Node writing to HBase instead of BigTable**
    - **Steps**:
        1. This setup uses `docker-compose.validator-to-hbase.yml` file.
        2. Run:
           ```bash
           docker compose -f ./docker-compose.validator-to-hbase.yml up -d --build
           ```
         3. The Solana Test Validator will now write data to HBase instead of BigTable. Use the Solana Lite RPC to query the data or exec into the HBase container to run HBase shell commands:
           ```bash
           docker compose -f ./docker-compose.validator-to-hbase.yml exec hbase /bin/bash
           docker compose -f ./docker-compose.validator-to-hbase.yml exec solana-lite-rpc /bin/bash 
           ```

2. **[Syncer](https://github.com/bwarelabs/solana-syncer) reading files written by [Solana Cos Plugin](https://github.com/bwarelabs/solana-cos-plugin) to Tencent Cloud Storage in sequencefiles format**
    - **Steps**:
        1. Update `config.properties` with Tencent Cloud Storage credentials.
        2. **IMPORTANT**: When running the Syncer in local mode, you need to have the both Solana node and [Solana Cos Plugin](https://github.com/bwarelabs/solana-cos-plugin) running and producing data to the local directory specified in the `config.properties` file. By default, the local directory in which the Syncer will look for files and where the Solana Cos Plugin will write the data is `/data`.
        3. Start the Syncer service with the `read-source=local-files` command:
           ```bash
           docker compose -f docker-compose.syncer-from-geyser-plugin-to-COS.yml up -d --build
           ```
        4. The Syncer will read local files and upload them to Tencent Cloud Storage.  

3. **Syncer migrating data from BigTable Emulator to Tencent Cloud Storage in sequencefiles format (for production, use real credentials)**  
    - **Steps**:
        1. Ensure your `config.properties` is updated with Tencent Cloud Storage credentials.
        2. Change `bigtable.project-id=test` to `bigtable.project-id=emulator`, and `bigtable.instance-id=test` to `bigtable.instance-id=solana-ledger` if you want to use the bigtable emulator as data source instead of a real BigTable instance.
        3. When using the BigTable Emulator, make sure you have data in BigTable. If not, generate some by running the Solana Test Validator and BigTable Emulator. 
           Run  
           ```bash
           docker compose -f docker-compose.syncer-from-bigtable-to-COS.yml up syncer-bigtable-using-emulator -d --build
           ```
        4. The Syncer will migrate data from BigTable Emulator to Tencent Cloud Storage in sequencefiles format.
