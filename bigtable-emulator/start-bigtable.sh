#!/bin/bash
echo "Bigtable emulator started"

gcloud beta emulators bigtable start --host-port=0.0.0.0:8086 &

while ! nc -z bigtable-emulator 8086; do
  echo "Waiting for BigTable Emulator to start..."
  sleep 1
done
echo "BigTable Emulator is up and running!"

agave/storage-bigtable/init-bigtable.sh

wait
