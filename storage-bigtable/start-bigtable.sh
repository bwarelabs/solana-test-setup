#!/bin/bash
echo "Bigtable emulator started"

gcloud beta emulators bigtable start --host-port=0.0.0.0:8086 &

# Wait for the emulator to start
sleep 3

agave/storage-bigtable/init-bigtable.sh

wait
