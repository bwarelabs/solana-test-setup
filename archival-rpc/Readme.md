# Solana Archival RPC

This folder contains a Docker-based setup for Solana Archival RPC. The service can be used to query the Solana network using data stored in HBase (it can use BigTable as well, but it's not the main focus of this repository).

It connects to the HBase database (using Apache Thrift) to read the data and expose it through a Solana RPC methods. (it does not support all the methods, check this repository for more information: [archival-rpc](https://github.com/dexterlaboss/archival-rpc))

You can query the Solana network using the Solana Archival RPC service by exec-ing into the docker service and sending requests to the `http://localhost:8899` endpoint.

## Usage
`curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"getHealth","id":1}' http://localhost:8899`

`curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"getBlock", "params": [0],"id":1}' http://localhost:8899`