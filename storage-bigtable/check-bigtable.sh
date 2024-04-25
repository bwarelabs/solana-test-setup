#!/bin/bash

while ! nc -z bigtable-emulator 8086; do
  echo "Waiting for BigTable Emulator to start..."
  sleep 1
done
echo "BigTable Emulator is up and running!"
